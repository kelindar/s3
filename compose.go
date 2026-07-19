package s3

import (
	"context"
	"fmt"
	"io/fs"
	"path"
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

	u := &uploader{Key: b.key, Client: b.Client, Bucket: b.bkt, Object: key}
	if err := u.Start(ctx); err != nil {
		return "", fmt.Errorf("s3 Compose: %w", err)
	}
	complete := false
	defer func() {
		if !complete {
			_ = u.Abort(context.WithoutCancel(ctx))
		}
	}()

	for i, part := range parts {
		if part.SourceKey = path.Clean(part.SourceKey); !fs.ValidPath(part.SourceKey) || part.ETag == "" || part.Offset < 0 || part.Size < MinPartSize {
			return "", fmt.Errorf("s3 Compose: invalid part %d", i+1)
		}
		source := &Reader{Key: b.key, Client: b.Client, Bucket: b.bkt, Path: part.SourceKey, ETag: part.ETag, Size: part.Offset + part.Size}
		if err := u.CopyFrom(ctx, int64(i+1), source, part.Offset, part.Offset+part.Size); err != nil {
			return "", fmt.Errorf("s3 Compose: part %d: %w", i+1, err)
		}
	}
	if err := u.Close(ctx, nil); err != nil {
		return "", fmt.Errorf("s3 Compose: %w", err)
	}
	complete = true
	return u.ETag(), nil
}
