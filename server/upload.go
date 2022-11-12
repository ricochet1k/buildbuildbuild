package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// returns true if path refers to a directory (possibly through a symlink)
func (job *RunningJob) UploadOutputPath(ctx context.Context, actionResult *execpb.ActionResult, path string) (bool, error) {
	stat, err := os.Lstat(filepath.Join(job.ExecRoot, path))
	if err != nil {
		return false, fmt.Errorf("Output missing %q: %v", path, err)
	}

	props := statToNodeProperties(stat)

	if stat.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(filepath.Join(job.ExecRoot, path))
		if err != nil {
			return false, fmt.Errorf("Output missing %q: %v", target, err)
		}

		isdir, err := job.UploadOutputPath(ctx, actionResult, target)
		if err != nil {
			return false, err
		}

		if isdir {
			outlink := &execpb.OutputSymlink{
				Path:           path,
				Target:         target,
				NodeProperties: props,
			}
			actionResult.OutputDirectorySymlinks = append(actionResult.OutputDirectorySymlinks, outlink)
		} else {
			outlink := &execpb.OutputSymlink{
				Path:           path,
				Target:         target,
				NodeProperties: props,
			}
			actionResult.OutputFileSymlinks = append(actionResult.OutputFileSymlinks, outlink)
		}

		return isdir, nil
	} else if stat.IsDir() {
		outdir := &execpb.OutputDirectory{
			Path: path,
			// TreeDigest:            &execpb.Digest{},
			IsTopologicallySorted: true,
		}
		actionResult.OutputDirectories = append(actionResult.OutputDirectories, outdir)
		job.eg.Go(func() error {
			digest, err := job.UploadTree(ctx, path)
			outdir.TreeDigest = digest
			return err
		})

		return true, nil
	} else {
		outfile := &execpb.OutputFile{
			Path: path,
			// Digest:         &execpb.Digest{},
			// IsExecutable:   false,
			// Contents:       []byte{},
			NodeProperties: props,
		}
		actionResult.OutputFiles = append(actionResult.OutputFiles, outfile)
		job.eg.Go(func() error {
			digest, stat, err := job.UploadFile(ctx, path)
			if err != nil {
				return err
			}
			outfile.Digest = digest
			outfile.IsExecutable = stat.Mode()&os.FileMode(0100) != 0
			return nil
		})

		return false, nil
	}
}

func (job *RunningJob) UploadTree(ctx context.Context, path string) (*execpb.Digest, error) {
	// We are manually generating this proto in topologically sorted order, partly to avoid the overhead
	// of proto.Marshal multiple times per directory. See execpb.OutputDirectory.IsTopologicallySorted for more detail.
	var alldirs []*execpb.Directory
	var allbytes []*[]byte
	_, err := job.UploadDirectory(ctx, &alldirs, &allbytes, path)
	if err != nil {
		return nil, err
	}

	totalBytesLen := 0
	for _, bs := range allbytes {
		totalBytesLen += len(*bs)
	}

	// var tree execpb.Tree
	// tree.Root = alldirs[0]
	// tree.Children = alldirs[1:]
	var buf bytes.Buffer
	buf.Grow(totalBytesLen)
	buf.WriteByte((1 << 3) | 2) // field 1, wiretype LEN
	buf.Write(*allbytes[0])
	for _, bs := range allbytes[1:] {
		buf.WriteByte((2 << 3) | 2) // field 2, wiretype LEN
		buf.Write(*bs)
	}

	treebytes := buf.Bytes()

	h := sha256.New()
	if _, err := io.Copy(h, bytes.NewReader(treebytes)); err != nil {
		return nil, err
	}

	hash := fmt.Sprintf("%x", h.Sum(nil))
	digest := &execpb.Digest{
		Hash:      hash,
		SizeBytes: int64(len(treebytes)),
	}

	job.eg.Go(func() error {
		_, err := job.c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Body:           &buf,
			Bucket:         &job.c.bucket,
			ChecksumSHA256: &hash,
			Key:            aws.String(StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(digest))),
			Metadata: map[string]*string{
				"B-Type": aws.String("Tree"),
				"B-Path": &path,
			},
		})
		return err
	})

	return digest, nil
}

func (job *RunningJob) UploadDirectory(ctx context.Context, alldirs *[]*execpb.Directory, allbytes *[]*[]byte, path string) (*execpb.Digest, error) {
	var dir execpb.Directory
	dirbytes := new([]byte)
	*alldirs = append(*alldirs, &dir)
	*allbytes = append(*allbytes, dirbytes)

	entries, err := os.ReadDir(filepath.Join(job.ExecRoot, path))
	if err != nil {
		return nil, err
	}

	eg, ctx := errgroup.WithContext(ctx)

	for _, entry := range entries {
		if entry.IsDir() {
			dirnode := &execpb.DirectoryNode{
				Name: entry.Name(),
				// Digest: digest,
			}
			dir.Directories = append(dir.Directories, dirnode)
			eg.Go(func() error {
				digest, err := job.UploadDirectory(ctx, alldirs, allbytes, filepath.Join(path, entry.Name()))
				if err != nil {
					return err
				}
				dirnode.Digest = digest
				return nil
			})
		} else {
			filenode := &execpb.FileNode{
				Name: entry.Name(),
				// Digest:         digest,
				// IsExecutable:   stat,
				// NodeProperties: &execpb.NodeProperties{},
			}
			dir.Files = append(dir.Files, filenode)
			eg.Go(func() error {
				digest, stat, err := job.UploadFile(ctx, filepath.Join(path, entry.Name()))
				if err != nil {
					return err
				}
				filenode.Digest = digest
				filenode.IsExecutable = stat.Mode()&fs.FileMode(0100) != 0
				filenode.NodeProperties = statToNodeProperties(stat)
				return nil
			})
		}
	}

	if err = eg.Wait(); err != nil {
		return nil, err
	}

	*dirbytes, err = proto.Marshal(&dir)
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, bytes.NewReader(*dirbytes)); err != nil {
		return nil, err
	}

	hash := fmt.Sprintf("%x", h.Sum(nil))
	digest := &execpb.Digest{
		Hash:      hash,
		SizeBytes: int64(len(*dirbytes)),
	}

	return digest, nil
}

func (job *RunningJob) UploadFile(ctx context.Context, path string) (*execpb.Digest, fs.FileInfo, error) {
	fullpath := filepath.Join(job.ExecRoot, path)
	f, err := os.Open(fullpath)
	if err != nil {
		return nil, nil, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return nil, nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}

	rawhash := h.Sum(nil)
	hash := fmt.Sprintf("%x", rawhash)
	digest := &execpb.Digest{
		Hash:      hash,
		SizeBytes: stat.Size(),
	}

	// move to cache so future jobs can reuse it without downloading
	if err := os.Rename(filepath.Join(job.ExecRoot, path), filepath.Join(job.c.config.CacheDir, DigestKey(digest))); err != nil {
		return nil, nil, err
	}

	if _, err = f.Seek(0, 0); err != nil {
		return nil, nil, err
	}

	job.eg.Go(func() error {
		defer f.Close()

		// check if it exists before uploading
		key := StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(digest))
		_, err := job.c.downloader.S3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: &job.c.bucket,
			Key:    &key,
		})
		if err == nil {
			return nil // already uploaded
		}

		hashb64 := base64.StdEncoding.EncodeToString(rawhash)

		start := time.Now()
		_, err = job.c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Body:           f,
			Bucket:         &job.c.bucket,
			ChecksumSHA256: &hashb64,
			Key:            aws.String(key),
			Metadata: map[string]*string{
				"B-Type": aws.String("File"),
				"B-Path": &path,
			},
		})
		if err != nil {
			return err
		}

		uploadDuration := time.Since(start)
		mbs := float64(digest.SizeBytes) / 1000000.0
		logrus.Printf("UploadFile  %.1f MB took %v (%.1f MB/s)  %v", mbs, uploadDuration, mbs/uploadDuration.Seconds(), path)
		return nil
	})

	return digest, stat, nil
}
