package storage

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type writerAtWriter struct {
	w   io.WriterAt
	off int64
}

func (w *writerAtWriter) Write(p []byte) (int, error) {
	n, err := w.w.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

// fast parallel download if there's a known size
func ParallelDownload(ctx context.Context, key BlobKey, size int, s Storage, f io.WriterAt, atomicDownloadProgress *int64) error {
	// The s3manager downloader downloads one Part first to see the total size of the file before starting parallel
	// downloads. Since we know the size already, we can download a bit faster than this one.

	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)

	partSize := 6 * 1024 * 1024
	if size/35 > partSize {
		partSize = size / 35
	}
	parts := size/partSize + 1

	for i := 0; i < parts; i++ {
		i := i
		startOffset := i * partSize
		eg.Go(func() error {
			thisPartSize := size - startOffset
			if thisPartSize > partSize {
				thisPartSize = partSize
			}
			reader, _, err := s.DownloadReader(egctx, key, startOffset, thisPartSize)
			if err != nil {
				return fmt.Errorf("Download part %v of %v: %w", i, key, err)
			}
			n, err := io.Copy(&writerAtWriter{f, int64(startOffset)}, reader)
			if atomicDownloadProgress != nil {
				atomic.AddInt64(atomicDownloadProgress, n)
			}
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		logrus.Errorf("ParallelDownload error: %v", err)
		return err
	}

	return nil
}
