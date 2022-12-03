package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
)

type ComboStorage struct {
	Cache Storage
	Main  Storage
}

func (s *ComboStorage) Exists(ctx context.Context, key BlobKey) (Metadata, error) {
	// logrus.Infof("Combo Exists %v", key)
	type metadataErr struct {
		md  Metadata
		err error
	}
	mainReader := make(chan metadataErr, 1)
	go func() {
		md, err := s.Main.Exists(ctx, key)
		mainReader <- metadataErr{md, err}
	}()

	md, err := s.Cache.Exists(ctx, key)
	if err != nil {
		// logrus.Warnf("Combo Cache exists err %q: %v", key, err)
		re := <-mainReader
		// if re.err != nil {
		// 	logrus.Warnf("Combo Main exists err %q: %v", key, err)
		// }
		return re.md, re.err
	}

	return md, err
}

// DownloadReader reads from cache if it exists, or s3 if not, but starts the s3 read at the same time to reduce latency
func (s *ComboStorage) DownloadReader(ctx context.Context, key BlobKey, offset, limit int) (io.ReadCloser, Metadata, error) {
	// logrus.Infof("Combo DownloadReader %v (%v %v)", key, offset, limit)
	if offset < 0 {
		panic("no")
	}
	type readerError struct {
		r   io.ReadCloser
		md  Metadata
		err error
	}
	mainReader := make(chan readerError, 1)
	go func() {
		r, md, err := s.Main.DownloadReader(ctx, key, offset, limit)
		mainReader <- readerError{r, md, err}
	}()

	r, md, err := s.Cache.DownloadReader(ctx, key, offset, limit)
	if err != nil {
		// if offset >= 0 {
		// 	logrus.Warnf("Combo Cache download reader err %q: %v", key, err)
		// }
		re := <-mainReader
		// if re.err != nil {
		// 	if offset >= 0 {
		// 		logrus.Warnf("Combo Main download reader err %q: %v", key, err)
		// 	}
		// }
		return re.r, re.md, re.err
	}

	return r, md, nil
}

// DownloadBytes races the cache and S3 and returns whichever one downloads first
func (s *ComboStorage) DownloadBytes(ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error) {
	type bufMetadataErr struct {
		buf *bytes.Buffer
		md  Metadata
		err error
	}
	mainReader := make(chan bufMetadataErr, 1)
	go func() {
		buf, md, err := s.Main.DownloadBytes(ctx, key)
		mainReader <- bufMetadataErr{buf, md, err}
	}()

	buf, md, err := s.Cache.DownloadBytes(ctx, key)
	if err != nil {
		// logrus.Warnf("Cache download bytes err %q: %v", key, err)
		re := <-mainReader
		return re.buf, re.md, re.err
	}

	return buf, md, err
}

type multiWriteCloser struct {
	a, b utils.WriteCloserFinish
}

func (m *multiWriteCloser) Write(p []byte) (n int, err error) {
	n, err = m.a.Write(p)
	if err != nil {
		return
	}

	n, err = m.b.Write(p)
	return
}

func (m *multiWriteCloser) Finish() error {
	aerr := m.a.Finish()
	berr := m.b.Finish()
	if aerr != nil {
		return aerr
	}
	return berr
}

func (m *multiWriteCloser) Close() error {
	aerr := m.a.Close()
	berr := m.b.Close()
	if aerr != nil {
		return aerr
	}
	return berr
}

// UploadWriter returns a writer that uploads to S3, and possibly also the cache if it is small
func (s *ComboStorage) UploadWriter(ctx context.Context, key BlobKey) (utils.WriteCloserFinish, error) {
	key.Filename()
	var writer utils.WriteCloserFinish

	w, err := s.Main.UploadWriter(ctx, key)
	if err != nil {
		return nil, err
	}
	writer = w

	if key.Size < 1*1024*1024 {
		cw, err := s.Cache.UploadWriter(ctx, key)
		if err != nil {
			logrus.Errorf("Unable to upload to cache %q: %v", key, err)
		} else {
			writer = &multiWriteCloser{writer, cw}
		}
	}

	return w, nil
}

func (s *ComboStorage) UploadBytes(ctx context.Context, key BlobKey, data []byte) error {
	key.Filename()
	go func() {
		err := s.Cache.UploadBytes(ctx, key, data)
		if err != nil {
			logrus.Errorf("Unable to upload to cache: %v", err)
		}
	}()

	return s.Main.UploadBytes(ctx, key, data)
}
