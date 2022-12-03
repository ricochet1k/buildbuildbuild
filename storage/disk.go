package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DiskStorage struct {
	Path    string
	BufPool *utils.BetterPool[*bytes.Buffer]
}

func (c *DiskStorage) CleanOldCacheFiles() {
	expireBefore := time.Now().Add(-2 * time.Hour)

	cleaned := c.DeleteFilesExpiringBefore(c.Path, expireBefore)

	if cleaned > 0 {
		logrus.Infof("Cleaned %v MB from the cache", utils.MB(cleaned))
	}
}

func (c *DiskStorage) DeleteFilesExpiringBefore(path string, expireBefore time.Time) int64 {
	entries, err := os.ReadDir(c.Path)
	if err != nil {
		logrus.Warnf("Unable to read cache dir for cleanup: %v", err)
		return 0
	}

	cleaned := int64(0)
	files := 0

	for _, entry := range entries {
		entryPath := filepath.Join(c.Path, entry.Name())
		if entry.IsDir() {
			cleaned += c.DeleteFilesExpiringBefore(entryPath, expireBefore)
			continue
		}

		stat, err := entry.Info()
		if err != nil {
			logrus.Warnf("Unable to stat cache entry: %v", err)
			continue
		}

		if stat.ModTime().Before(expireBefore) {
			if err := os.Remove(entryPath); err != nil {
				logrus.Warnf("Unable to remove cache entry: %v", err)
				continue
			}

			cleaned += stat.Size()
		} else {
			files += 1
		}
	}

	if files == 0 && path != c.Path {
		if err := os.Remove(path); err != nil {
			logrus.Warnf("Unable to remove empty cache dir: %v", err)
		}
	}

	return cleaned
}

func (c *DiskStorage) MaybeUpdateMetadata(ctx context.Context, key BlobKey, existingMetadata Metadata) error {
	if key.NeedsMetadataUpdate(existingMetadata) {
		return c.WriteMetadata(ctx, filepath.Join(c.Path, key.InstancePath()), key.Metadata)
	}
	return nil
}

func (c *DiskStorage) Exists(ctx context.Context, key BlobKey) (Metadata, error) {
	_, md, err := c.DownloadReader(ctx, key, -1, 1)
	return md, err
}

type readerCloser struct {
	io.Reader
	io.Closer
}

func (c *DiskStorage) DownloadReader(ctx context.Context, key BlobKey, offset, limit int) (io.ReadCloser, Metadata, error) {
	path := filepath.Join(c.Path, key.InstancePath())

	if key.Size == 0 {
		// we are required to assume that 0 size blobs exist
		f, err := os.Create(path)
		if err != nil {
			return nil, nil, utils.ErrorToStatus(fmt.Errorf("Disk.DownloadReader Open (Size 0 create): %w", err)).Err()
		}
		f.Close()
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, utils.ErrorToStatus(fmt.Errorf("Disk.DownloadReader Open %v: %w", offset, err)).Err()
	}
	if key.Size >= 0 {
		stat, err := f.Stat()
		if err != nil {
			return nil, nil, utils.ErrorToStatus(fmt.Errorf("Stat: %w", err)).Err()
		}
		if key.Compressor == 0 && stat.Size() != int64(key.Size) {
			logrus.Warnf("On-disk file is not the right size! %q  %v != %v", path, stat.Size(), key.Size)
			f.Close()
			os.Remove(path)
			return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("Wrong size: %v != %v", stat.Size(), key.Size))
		}
	}

	md, err := c.ReadMetadata(ctx, path)
	if err != nil {
		logrus.Warnf("[disk] Unable to read metadata for %q: %v", path, err)
	}

	reader := io.ReadCloser(f)

	if offset > 0 {
		if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	if limit > 0 {
		reader = readerCloser{
			Reader: io.LimitReader(f, int64(limit)),
			Closer: f,
		}
	}

	if (key.Metadata != nil && len(key.Metadata) > 0) || key.ExpiresMin > 0 {
		// since this function is called in Race, we don't want to cancel the metadata update
		// so we are not using the parent context (maybe we should detach instead?)
		go c.MaybeUpdateMetadata(context.TODO(), key, md)
	}

	if utils.DebugFilter(path) {
		logrus.Debugf("Disk Read: %v", path)
	}

	return reader, md, nil
}

func (c *DiskStorage) DownloadBytes(ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error) {
	buf, md, err := downloadBytes(c, ctx, key)
	return buf, md, utils.ErrorToStatus(err).Err()
}

func (c *DiskStorage) ReadMetadata(ctx context.Context, path string) (Metadata, error) {
	mdf, err := os.Open(path + ".metadata")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, utils.ErrorToStatus(err).Err()
	}
	defer mdf.Close()

	mdd := gob.NewDecoder(mdf)
	var md map[string]*string
	err = mdd.Decode(&md)
	if err != nil {
		if len(md) > 0 {
			logrus.Warnf("Metadata so far: %q", md)
		}
		stat, _ := mdf.Stat()
		mdf.Seek(0, io.SeekStart)
		var bytes bytes.Buffer
		io.Copy(&bytes, mdf)
		return nil, fmt.Errorf("gob decode: %v. %q %w", stat.Size(), string(bytes.Bytes()), err)
	}
	return Metadata(md), err
}

func (c *DiskStorage) WriteMetadata(ctx context.Context, path string, metadata Metadata) error {
	if len(metadata) == 0 {
		return nil
	}

	mdpath := path + ".metadata"
	mdpathupload := fmt.Sprintf("%v.upload%v", mdpath, rand.Intn(4096))
	mdf, err := os.Create(mdpathupload)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return utils.ErrorToStatus(err).Err()
	}
	defer mdf.Close()
	mdd := gob.NewEncoder(mdf)
	err = mdd.Encode(map[string]*string(metadata))
	if err != nil {
		os.Remove(mdpathupload)
		logrus.Errorf("gob encode: %q %v", metadata, err)
		return fmt.Errorf("gob encode: %w", err)
	}
	mdf.Close()
	return os.Rename(mdpathupload, mdpath)
}

func (c *DiskStorage) UploadWriter(ctx context.Context, key BlobKey) (utils.WriteCloserFinish, error) {
	path := filepath.Join(c.Path, key.InstancePath())

	err := os.MkdirAll(filepath.Dir(path), fs.FileMode(0755))
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, utils.ErrorToStatus(err).Err()
	}

	f, err := utils.CreateAtomic(path)
	if err != nil {
		logrus.Errorf("Disk.UploadWriter Create %q: %v", path, err)
		return nil, utils.ErrorToStatus(fmt.Errorf("Disk.UploadWriter Create: %w", err)).Err()
	}

	if key.Metadata != nil && len(key.Metadata) > 0 {
		if err := c.WriteMetadata(ctx, path, key.Metadata); err != nil {
			f.Close()
			return nil, err
		}
	}

	if utils.DebugFilter(path) {
		logrus.Debugf("Disk Write: %v", path)
	}

	return f, nil
}

// Upload to the cluster cache
func (c *DiskStorage) UploadBytes(ctx context.Context, key BlobKey, data []byte) error {
	err := uploadBytes(c, ctx, key, data)
	return utils.ErrorToStatus(err).Err()
}
