package server

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	wrapperspb "github.com/golang/protobuf/ptypes/wrappers"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Server) CleanOldCacheFiles() {
	expireBefore := time.Now().Add(-2 * time.Hour)

	cleaned := int64(0)

	entries, err := os.ReadDir(c.config.CacheDir)
	if err != nil {
		logrus.Warnf("Unable to read cache dir for cleanup: %v", err)
		return
	}

	for _, entry := range entries {
		stat, err := entry.Info()
		if err != nil {
			logrus.Warnf("Unable to stat cache entry: %v", err)
			continue
		}

		if stat.ModTime().Before(expireBefore) {
			if err := os.Remove(filepath.Join(c.config.CacheDir, entry.Name())); err != nil {
				logrus.Warnf("Unable to remove cache entry: %v", err)
				continue
			}

			cleaned += stat.Size()
		}
	}

	if cleaned > 0 {
		logrus.Infof("Cleaned %v MB from the cache", float64(cleaned)/1000000)
	}
}

func (job *RunningJob) DownloadAll(ctx context.Context, path string, dirDigest *execpb.Digest) error {
	var dir execpb.Directory
	if err := job.c.DownloadProto(ctx, StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(dirDigest)), &dir); err != nil {
		logrus.Printf("Worker cannot get dir: %v", err)

		return err
	}

	mode := uint32(0755)
	if dir.NodeProperties != nil && dir.NodeProperties.UnixMode != nil {
		mode = dir.NodeProperties.UnixMode.Value
	}
	if err := os.Mkdir(filepath.Join(job.ExecRoot, path), os.FileMode(mode)); err != nil && !os.IsExist(err) {
		return fmt.Errorf("Mkdir %q: %w", path, err)
	}
	// logrus.Printf("Mkdir %q: %q", path, filepath.Join(job.ExecRoot, path))

	if err := job.ApplyNodeProperties(ctx, path, dir.NodeProperties); err != nil {
		return err
	}

	for _, subdir := range dir.Directories {
		subdir := subdir
		job.eg.Go(func() error {
			return job.DownloadAll(ctx, filepath.Join(path, subdir.Name), subdir.Digest)
		})
	}

	for _, file := range dir.Files {
		file := file
		job.eg.Go(func() error {
			return job.DownloadFile(ctx, filepath.Join(path, file.Name), file)
		})
	}

	for _, symlink := range dir.Symlinks {
		sympath := filepath.Join(path, symlink.Name)
		if err := os.Symlink(filepath.Join(job.ExecRoot, symlink.Target), filepath.Join(job.ExecRoot, sympath)); err != nil {
			return fmt.Errorf("Symlink %q: %w", sympath, err)
		}

		if err := job.ApplyNodeProperties(ctx, sympath, symlink.NodeProperties); err != nil {
			return err
		}
	}

	return nil
}

func (job *RunningJob) ApplyNodeProperties(ctx context.Context, path string, props *execpb.NodeProperties) error {
	// mode is set during creation
	if props == nil {
		return nil
	}

	if props.Mtime != nil {
		if err := os.Chtimes(filepath.Join(job.ExecRoot, path), props.Mtime.AsTime(), props.Mtime.AsTime()); err != nil {
			return fmt.Errorf("Chtimes %q: %w", path, err)
		}
	}

	return nil
}

func statToNodeProperties(stat fs.FileInfo) *execpb.NodeProperties {
	return &execpb.NodeProperties{
		Properties: nil,
		Mtime:      timestamppb.New(stat.ModTime()),
		UnixMode:   &wrapperspb.UInt32Value{Value: uint32(stat.Mode())},
	}
}

func (job *RunningJob) DownloadFile(ctx context.Context, path string, fileNode *execpb.FileNode) error {
	dkey := DigestKey(fileNode.Digest)

	// first check if anything else is downloading already
	downloaded := make(chan struct{})
	defer close(downloaded)
	downloadedChan, alreadyDownloading := job.c.downloading.LoadOrStore(dkey, downloaded)
	if alreadyDownloading {
		// logrus.Printf("Already downloading %v...", path)
		<-downloadedChan.(chan struct{})
	} else {
		defer job.c.downloading.Delete(dkey)
	}

	cachepath := filepath.Join(job.c.config.CacheDir, dkey)

	// then check if it's already been downloaded
	cachefile, err := os.Open(cachepath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Check cache: %w", err)
	}

	if err != nil { // need to download to cache
		err = job.DownloadFileToCache(ctx, cachepath, dkey, fileNode.Digest.SizeBytes)
		if err != nil {
			return err
		}

		cachefile, err = os.Open(cachepath)
		if err != nil {
			return fmt.Errorf("After downloading: %w", err)
		}
		defer cachefile.Close()
	}

	mode := uint32(0644)
	if fileNode.NodeProperties != nil && fileNode.NodeProperties.UnixMode != nil {
		mode = fileNode.NodeProperties.UnixMode.Value
	}
	if fileNode.IsExecutable {
		mode |= 0111
	}
	f, err := os.OpenFile(filepath.Join(job.ExecRoot, path), os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(mode))
	if err != nil {
		return fmt.Errorf("OpenFile failed %q: %w", path, err)
	}
	defer f.Close()

	// We could os.Link (hardlink) but then jobs could modify the cache, so it's safer to just copy.
	if _, err = io.Copy(f, cachefile); err != nil {
		return fmt.Errorf("Copy failed %q: %w", path, err)
	}

	if err := job.ApplyNodeProperties(ctx, path, fileNode.NodeProperties); err != nil {
		return err
	}

	return nil
}

func (job *RunningJob) DownloadFileToCache(ctx context.Context, cachepath string, digestKey string, size int64) error {
	// logrus.Printf("DownloadFileToCache %v", cachepath)

	dlpath := cachepath + ".dl"

	f, err := os.OpenFile(dlpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("OpenFile failed %q: %w", dlpath, err)
	}
	defer f.Close()

	if size > 0 {
		key := StorageKey(job.InstanceName, CONTENT_CAS, digestKey)

		atomic.AddInt64(&job.inputBytes, size)
		job.c.downloadConcurrency <- struct{}{}
		defer func() { <-job.c.downloadConcurrency }()

		start := time.Now()

		// This downloader downloads one Part first to see the total size of the file before starting parallel
		// downloads. Since we know the size already, we could download a bit faster than this one.
		// downloader := s3manager.NewDownloader(job.c.sess, func(d *s3manager.Downloader) { d.PartSize = 20 * 1000 * 1000 })
		n, err := job.c.downloader.DownloadWithContext(ctx, f, &s3.GetObjectInput{
			Bucket: &job.c.bucket,
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("Download %q: %w", key, err)
		}
		atomic.AddInt64(&job.downloadedBytes, n)

		dur := time.Since(start)
		mbsize := float64(size) / 1000000
		log.Printf("DownloadFileToCache %v: %.1f MB took %v (%.1f MB/s)", cachepath, mbsize, dur, mbsize/dur.Seconds())
	}

	if err := os.Rename(dlpath, cachepath); err != nil {
		return fmt.Errorf("Rename %q: %w", cachepath, err)
	}
	return nil
}
