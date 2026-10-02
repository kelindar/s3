package s3

import (
	"context"
	"fmt"
	"io/fs"
	"path"

	"golang.org/x/sync/errgroup"
)

// CopyPart describes an immutable byte range to copy into a composed object.
type CopyPart struct {
	SourceKey string
	ETag      string
	Offset    int64
	Size      int64
}

// Compose concatenates immutable source ranges using multipart server-side copy.
func (b *Bucket) Compose(ctx context.Context, key string, parts []CopyPart) (string, error) {
	key = path.Clean(key)
	switch {
	case !fs.ValidPath(key) || key == ".":
		return "", badpath("s3 Compose", key)
	case len(parts) == 0 || len(parts) > MaxParts:
		return "", fmt.Errorf("s3 Compose: invalid part count %d", len(parts))
	}
	for i, part := range parts {
		part.SourceKey = path.Clean(part.SourceKey)
		switch {
		case !fs.ValidPath(part.SourceKey) || part.SourceKey == "." || part.ETag == "" || part.Offset < 0 || part.Size < MinPartSize:
			return "", fmt.Errorf("s3 Compose: invalid part %d", i+1)
		case part.Offset > (1<<63-1)-part.Size:
			return "", fmt.Errorf("s3 Compose: invalid part %d range", i+1)
		}
	}

	u := &uploader{Key: b.key, Bucket: b.bkt, Object: key}
	if err := u.Start(ctx); err != nil {
		return "", fmt.Errorf("s3 Compose: %w", err)
	}
	u.parts = make([]tagpart, 0, len(parts))
	complete := false
	defer func() {
		if !complete {
			_ = u.Abort(context.WithoutCancel(ctx))
		}
	}()

	g, copyCtx := errgroup.WithContext(ctx)
	g.SetLimit(40)
	for i, part := range parts {
		if copyCtx.Err() != nil {
			break
		}
		part.SourceKey = path.Clean(part.SourceKey)
		source := &Reader{Key: b.key, Bucket: b.bkt, Path: part.SourceKey, ETag: part.ETag, Size: part.Offset + part.Size}
		g.Go(func() error {
			if err := u.CopyFrom(copyCtx, int64(i+1), source, part.Offset, part.Offset+part.Size); err != nil {
				return fmt.Errorf("s3 Compose: part %d: %w", i+1, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return "", err
	}
	if err := u.Close(ctx, nil); err != nil {
		return "", fmt.Errorf("s3 Compose: %w", err)
	}
	complete = true
	return u.ETag(), nil
}
