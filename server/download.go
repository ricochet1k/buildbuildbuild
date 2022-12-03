package server

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	wrapperspb "github.com/golang/protobuf/ptypes/wrappers"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ErrorWithMissingDigests struct {
	error
	MissingDigests []*execpb.Digest
}

func (e ErrorWithMissingDigests) Unwrap() error {
	return e.error
}

func (e ErrorWithMissingDigests) Error() string {
	return "ErrorWithMissingDigests: " + e.error.Error()
}

func (job *RunningJob) AddMissing(missingDigests ...*execpb.Digest) {
	job.missingBlobsMutex.Lock()
	defer job.missingBlobsMutex.Unlock()
	job.missingBlobs = append(job.missingBlobs, missingDigests...)
}

func (job *RunningJob) DownloadAll(ctx context.Context, path string, dirDigest *execpb.Digest) error {
	key := storage.BlobKey{InstanceName: job.InstanceName, Kind: storage.CONTENT_CAS, Digest: dirDigest.Hash, Size: int(dirDigest.SizeBytes)}
	var dir execpb.Directory
	if err := job.c.DownloadProto(ctx, key, &dir); err != nil {
		logrus.Errorf("Worker cannot get dir %v: %v", key, err)

		if status.Code(err) == codes.NotFound {
			job.AddMissing(dirDigest)
		}

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
	key := storage.BlobKey{InstanceName: job.InstanceName, Kind: storage.CONTENT_CAS, Digest: fileNode.Digest.Hash, Size: int(fileNode.Digest.SizeBytes)}
	cachepath := job.c.CachePath(key)

	// then check if it's already been downloaded
	cachefile, err := os.Open(cachepath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Check cache: %w", err)
	}

	if err != nil { // need to download to cache
		filename := key.InstancePath()

		// first check if anything else is downloading already
		downloaded := make(chan struct{})
		defer close(downloaded)
		downloadedChan, alreadyDownloading := job.c.downloading.LoadOrStore(filename, downloaded)
		if alreadyDownloading {
			// logrus.Printf("Already downloading %v...", path)
			<-downloadedChan.(chan struct{})
		} else {
			defer job.c.downloading.Delete(filename)

			err = job.DownloadFileToCache(ctx, cachepath, key, int(fileNode.Digest.SizeBytes))
			if err != nil {
				st := utils.ErrorToStatus(err)
				if st.Code() == codes.NotFound {
					job.AddMissing(fileNode.Digest)
				}
				return st.Err()
			}
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

func (job *RunningJob) DownloadFileToCache(ctx context.Context, cachepath string, key storage.BlobKey, size int) (finalerr error) {
	// logrus.Printf("DownloadFileToCache %v %v", cachepath, size)

	if err := os.MkdirAll(filepath.Dir(cachepath), fs.FileMode(0755)); err != nil {
		return err
	}

	f, err := utils.CreateAtomic(cachepath)
	if err != nil {
		return fmt.Errorf("DownloadFileToCache Create failed %q: %w", cachepath, err)
	}
	defer f.Close()

	if size > 0 {
		atomic.AddInt64(&job.inputBytes, int64(size))
		job.c.downloadConcurrency <- struct{}{}
		defer func() { <-job.c.downloadConcurrency }()

		start := time.Now()

		if job.c.config.Compress {
			r, _, err := job.c.DownloadReader(ctx, key, 0, 0)
			if err != nil {
				return err
			}

			n, err := io.Copy(f, r)
			if err != nil {
				return err
			}
			atomic.AddInt64(&job.downloadedBytes, int64(n))

		} else {
			err := storage.ParallelDownload(ctx, key, size, job.c.Storage, f, &job.downloadedBytes)
			if err != nil {
				return err
			}
		}

		dur := time.Since(start)
		mbsize := float64(size) / 1000000.0
		if dur > 1*time.Second {
			logrus.Infof("DownloadFileToCache %v: %.1f MB took %v (%.1f MB/s)", cachepath, mbsize, dur, mbsize/dur.Seconds())
		}
	}

	if err := f.Finish(); err != nil {
		return fmt.Errorf("Finish %q: %w", cachepath, err)
	}
	return nil
}
