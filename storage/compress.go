package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Compressed struct {
	inner             Storage
	minSize           int
	storageCompressor execpb.Compressor_Value
	encoderPool       utils.BetterPool[*zstd.Encoder]
	decoderPool       utils.BetterPool[*zstd.Decoder]
}

func NewCompressed(inner Storage, storageCompressor execpb.Compressor_Value) *Compressed {
	return &Compressed{
		inner:             inner,
		minSize:           2 * 1024,
		storageCompressor: storageCompressor,
		encoderPool: utils.NewPool(func() *zstd.Encoder {
			w, _ := zstd.NewWriter(nil)
			return w
		}, func(*zstd.Encoder) {}),
		decoderPool: utils.NewPool(func() *zstd.Decoder {
			r, _ := zstd.NewReader(nil)
			return r
		}, func(*zstd.Decoder) {}),
	}
}

var (
	_ = Storage(&Compressed{})
)

func (c *Compressed) getStorageCompressor(key BlobKey) execpb.Compressor_Value {
	if key.Size >= 0 && key.Size < c.minSize {
		return execpb.Compressor_IDENTITY
	}
	return c.storageCompressor
}

func (c *Compressed) Exists(ctx context.Context, key BlobKey) (Metadata, error) {
	key.Compressor = c.getStorageCompressor(key)
	return c.inner.Exists(ctx, key)
}

type pooledDecoder struct {
	io.Reader
	decoder *zstd.Decoder
	inner   io.Closer
	pool    *utils.BetterPool[*zstd.Decoder]
}

func (p *pooledDecoder) Close() error {
	if p.decoder != nil {
		// p.decoder.Close() // cannot reuse after this
		p.inner.Close()
		p.pool.Put(p.decoder)
		p.decoder = nil
		p.inner = nil
		p.pool = nil
	}
	return nil

}

func (c *Compressed) DownloadBytes(ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error) {
	return downloadBytes(c, ctx, key)
}

func (c *Compressed) DownloadReader(ctx context.Context, key BlobKey, offset int, limit int) (io.ReadCloser, Metadata, error) {
	storageCompressor := c.getStorageCompressor(key)
	wantCompressor := key.Compressor
	if utils.DebugFilter(key.Filename()) {
		logrus.Debugf("Compressed download %v (%v %v): %v <- %v", key, offset, limit, wantCompressor, storageCompressor)
	}
	if wantCompressor == storageCompressor {
		// pass-through, no need to decompress
		return c.inner.DownloadReader(ctx, key, offset, limit)
	}

	key.Compressor = storageCompressor
	reader, md, err := c.inner.DownloadReader(ctx, key, 0, 0) // zstd isn't seekable
	if err != nil {
		return nil, md, err
	}

	if wantCompressor == execpb.Compressor_IDENTITY {
		decoder := c.decoderPool.Get()
		decoder.Reset(reader)

		reader = &pooledDecoder{
			Reader:  decoder,
			decoder: decoder,
			inner:   reader,
			pool:    &c.decoderPool,
		}
	} else if wantCompressor == execpb.Compressor_ZSTD {
		// we already know that wantCompressor != storageCompressor

		encoder := c.encoderPool.Get()
		pr, pw := io.Pipe()
		encoder.Reset(pw)
		origReader := reader
		go func() {
			_, err := encoder.ReadFrom(origReader)
			encoder.Close()
			reader.Close()
			pw.CloseWithError(err)
			c.encoderPool.Put(encoder)
		}()

		reader = pr
	} else {
		logrus.Errorf("Invalid compressor: %v", wantCompressor)
		return nil, md, status.Error(codes.InvalidArgument, fmt.Sprintf("Invalid compressor: %v", wantCompressor))
	}

	if offset > 0 {
		// This is a pain. Zstd isn't seekable. Maybe we should take the complicated approach
		// of using the seekable zstd impl and storing a separate block manifest for faster seeking? :/
		logrus.Warnf("Compressed offset by reading to offset! %v %v %v %v", key, wantCompressor, offset, limit)

		skipped := 0

		var buf [1024]byte
		for offset-skipped > 1024 {
			n, err := reader.Read(buf[:])
			if err != nil {
				return nil, nil, err
			}
			skipped += n
		}

		n, err := reader.Read(buf[:offset-skipped])
		if err != nil {
			return nil, nil, err
		}
		skipped += n
	}

	if limit > 0 {
		reader = readerCloser{io.LimitReader(reader, int64(limit)), reader}
	}

	return reader, md, err
}

func (c *Compressed) UploadBytes(ctx context.Context, key BlobKey, data []byte) error {
	return uploadBytes(c, ctx, key, data)
}

type pooledEncoder struct {
	*zstd.Encoder
	inner utils.WriteCloserFinish
	pool  *utils.BetterPool[*zstd.Encoder]
}

func (p *pooledEncoder) Finish() error {
	if p.Encoder != nil {
		if err := p.Encoder.Close(); err != nil {
			return err
		}
		if err := p.inner.Finish(); err != nil {
			return err
		}
		p.pool.Put(p.Encoder)
		p.Encoder = nil
		p.pool = nil
	}
	return nil
}

func (p *pooledEncoder) Close() error {
	if p.Encoder != nil {
		if err := p.Encoder.Close(); err != nil {
			return err
		}
		p.pool.Put(p.Encoder)
		p.Encoder = nil
	}
	if p.inner != nil {
		if err := p.inner.Close(); err != nil {
			return err
		}
		p.inner = nil
	}
	return nil
}

type writerFinisher struct {
	io.WriteCloser // pipe
	inner          utils.WriteCloserFinish
	decoder        *zstd.Decoder
	writeCompleted <-chan error
	key            BlobKey
}

func (w *writerFinisher) Finish() error {
	err := w.WriteCloser.Close()
	if err != nil {
		return err
	}
	err = <-w.writeCompleted
	if err != nil {
		return err
	}
	if utils.DebugFilter(w.key.Filename()) {
		logrus.Debugf("writerFinisher Finish! %v", w.key)
	}
	return w.inner.Finish()
}

func (w *writerFinisher) Close() error {
	w.WriteCloser.Close()
	if utils.DebugFilter(w.key.Filename()) {
		logrus.Debugf("writerFinisher Close! %v", w.key)
	}
	return w.inner.Close()
}

func (c *Compressed) UploadWriter(ctx context.Context, key BlobKey) (utils.WriteCloserFinish, error) {
	storageCompressor := c.getStorageCompressor(key)
	incomingCompressor := key.Compressor
	if utils.DebugFilter(key.Filename()) {
		logrus.Debugf("Compressed upload %v: %v -> %v", key, incomingCompressor, storageCompressor)
	}
	if incomingCompressor == storageCompressor {
		// pass-through, no need to compress
		return c.inner.UploadWriter(ctx, key)
	}

	key.Compressor = storageCompressor
	writer, err := c.inner.UploadWriter(ctx, key)
	if err != nil {
		return nil, err
	}

	if incomingCompressor == execpb.Compressor_IDENTITY {
		encoder := c.encoderPool.Get()
		encoder.Reset(writer)

		writer = &pooledEncoder{
			Encoder: encoder,
			inner:   writer,
			pool:    &c.encoderPool,
		}
	} else if incomingCompressor == execpb.Compressor_ZSTD {
		decoder := c.decoderPool.Get()
		pr, pw := io.Pipe()
		decoder.Reset(pr)
		origWriter := writer
		writeCompleted := make(chan error, 1)
		go func() {
			_, err := decoder.WriteTo(origWriter)
			writeCompleted <- err
			close(writeCompleted)
			decoder.Reset(nil)
			c.decoderPool.Put(decoder)
		}()

		writer = &writerFinisher{pw, writer, decoder, writeCompleted, key}
	} else {
		logrus.Errorf("Invalid compressor: %v", incomingCompressor)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Invalid compressor: %v", incomingCompressor))
	}

	return writer, err
}
