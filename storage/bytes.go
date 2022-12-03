package storage

import (
	"bytes"
	"context"
	"io"
)

func downloadBytes(s Storage, ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error) {
	r, metadata, err := s.DownloadReader(ctx, key, 0, 0)
	if err != nil {
		return nil, nil, err
	}

	buf := &bytes.Buffer{} // c.BufPool.Get()
	if _, err := io.Copy(buf, r); err != nil {
		// s.bufPool.Put(buf)
		return nil, nil, err
	}

	return buf, metadata, nil
}

func uploadBytes(s Storage, ctx context.Context, key BlobKey, data []byte) error {
	w, err := s.UploadWriter(ctx, key)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err == nil {
		err = w.Finish()
	}
	if err != nil {
		w.Close()
		return err
	}

	return w.Close()
}
