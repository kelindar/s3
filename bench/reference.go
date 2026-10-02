package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/kelindar/s3"
	"golang.org/x/sync/errgroup"
)

// The caller closes idle connections after all bodies and requests have finished.
func reference(endpoint string) (*sdk.Client, func()) {
	transport := &http.Transport{}
	client := sdk.New(sdk.Options{
		Region:                     region,
		Credentials:                credentials.NewStaticCredentialsProvider("bench-access", "bench-secret", ""),
		BaseEndpoint:               sdkaws.String(endpoint),
		UsePathStyle:               true,
		Retryer:                    sdkaws.NopRetryer{},
		HTTPClient:                 &http.Client{Transport: transport, Timeout: 30 * time.Second},
		RequestChecksumCalculation: sdkaws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: sdkaws.ResponseChecksumValidationWhenRequired,
		APIOptions: []func(*middleware.Stack) error{func(stack *middleware.Stack) error {
			// Match the library's unsigned bodies even though the mock uses HTTP.
			switch stack.ID() {
			case "PutObject", "UploadPart", "CompleteMultipartUpload":
				return v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware(stack)
			}
			return nil
		}},
	})
	return client, transport.CloseIdleConnections
}

func referenceList(ctx context.Context, client *sdk.Client, prefix string) ([]types.Object, error) {
	pages := sdk.NewListObjectsV2Paginator(client, &sdk.ListObjectsV2Input{
		Bucket: sdkaws.String(bucketName), Prefix: sdkaws.String(prefix), Delimiter: sdkaws.String("/"), EncodingType: types.EncodingTypeUrl,
	})
	var objects []types.Object
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing reference objects: %w", err)
		}
		if len(objects) == 0 {
			objects = page.Contents
		} else {
			objects = append(objects, page.Contents...)
		}
	}
	return objects, nil
}

// referenceUpload matches WriteFrom's part size and concurrency for these fixtures.
// The trailing partial part follows the full parts, and completed slots stay ordered.
func referenceUpload(ctx context.Context, client *sdk.Client, object string, reader io.ReaderAt, size int64) error {
	if size < s3.MinPartSize {
		_, err := client.PutObject(ctx, &sdk.PutObjectInput{
			Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), Body: io.NewSectionReader(reader, 0, size), ContentLength: sdkaws.Int64(size),
		})
		return err
	}
	created, err := client.CreateMultipartUpload(ctx, &sdk.CreateMultipartUploadInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object)})
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_, _ = client.AbortMultipartUpload(context.WithoutCancel(ctx), &sdk.AbortMultipartUploadInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), UploadId: created.UploadId})
		}
	}()
	parts := make([]types.CompletedPart, (size+s3.MinPartSize-1)/s3.MinPartSize)
	upload := func(ctx context.Context, i int) error {
		offset := int64(i) * s3.MinPartSize
		length := min(int64(s3.MinPartSize), size-offset)
		part, err := client.UploadPart(ctx, &sdk.UploadPartInput{
			Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), UploadId: created.UploadId, PartNumber: sdkaws.Int32(int32(i + 1)),
			Body: io.NewSectionReader(reader, offset, length), ContentLength: sdkaws.Int64(length),
		})
		if err != nil {
			return err
		}
		parts[i] = types.CompletedPart{ETag: part.ETag, PartNumber: sdkaws.Int32(int32(i + 1))}
		return nil
	}
	g, uploadCtx := errgroup.WithContext(ctx)
	g.SetLimit(40)
	full := int(size / s3.MinPartSize)
	for i := range full {
		g.Go(func() error { return upload(uploadCtx, i) })
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if size%s3.MinPartSize != 0 {
		if err := upload(ctx, full); err != nil {
			return err
		}
	}
	_, err = client.CompleteMultipartUpload(ctx, &sdk.CompleteMultipartUploadInput{
		Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	complete = err == nil
	return err
}

func referenceCompose(ctx context.Context, client *sdk.Client, object string, sources []s3.CopyPart) (string, error) {
	created, err := client.CreateMultipartUpload(ctx, &sdk.CreateMultipartUploadInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object)})
	if err != nil {
		return "", err
	}
	complete := false
	defer func() {
		if !complete {
			_, _ = client.AbortMultipartUpload(context.WithoutCancel(ctx), &sdk.AbortMultipartUploadInput{Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), UploadId: created.UploadId})
		}
	}()
	parts := make([]types.CompletedPart, len(sources))
	g, copyCtx := errgroup.WithContext(ctx)
	g.SetLimit(40)
	for i, source := range sources {
		g.Go(func() error {
			part, err := client.UploadPartCopy(copyCtx, &sdk.UploadPartCopyInput{
				Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), UploadId: created.UploadId, PartNumber: sdkaws.Int32(int32(i + 1)),
				CopySource: sdkaws.String("/" + bucketName + "/" + url.PathEscape(source.SourceKey)), CopySourceIfMatch: sdkaws.String(source.ETag),
				CopySourceRange: sdkaws.String(fmt.Sprintf("bytes=%d-%d", source.Offset, source.Offset+source.Size-1)),
			})
			if err != nil {
				return err
			}
			parts[i] = types.CompletedPart{ETag: part.CopyPartResult.ETag, PartNumber: sdkaws.Int32(int32(i + 1))}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return "", err
	}
	result, err := client.CompleteMultipartUpload(ctx, &sdk.CompleteMultipartUploadInput{
		Bucket: sdkaws.String(bucketName), Key: sdkaws.String(object), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		return "", err
	}
	complete = true
	return sdkaws.ToString(result.ETag), nil
}
