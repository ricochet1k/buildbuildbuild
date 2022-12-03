package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/ricochet1k/buildbuildbuild/utils"
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
			digest, stat, err := job.UploadFile(ctx, path, job.eg)
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
	_, err := job.TreeUploadDirectory(ctx, &alldirs, &allbytes, path)
	if err != nil {
		return nil, err
	}

	totalBytesLen := 0
	for _, bs := range allbytes {
		totalBytesLen += 1 + 4 + len(*bs)
	}

	// var tree execpb.Tree
	// tree.Root = alldirs[0]
	// tree.Children = alldirs[1:]
	var buf bytes.Buffer
	buf.Grow(totalBytesLen)
	for i, bs := range allbytes {
		if i == 0 {
			buf.WriteByte((1 << 3) | 2) // field 1, wiretype LEN
		} else {
			buf.WriteByte((2 << 3) | 2) // field 2, wiretype LEN
		}
		buf.Write(proto.EncodeVarint(uint64(len(*bs)))) // length prefix
		buf.Write(*bs)
	}

	treebytes := buf.Bytes()

	//sanity check that we can unmarshal it
	var tree execpb.Tree
	if err := proto.Unmarshal(treebytes, &tree); err != nil {
		logrus.Panicf("UploadTree tried to generate an invalid proto: %v.  %q", err, treebytes)
	}

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
		key := storage.BlobKey{
			InstanceName: job.InstanceName,
			Kind:         storage.CONTENT_CAS,
			Digest:       hash,
			Size:         len(treebytes),
			ExpiresMin:   12 * time.Hour,
		}
		key.Metadata.Put("proto", fmt.Sprintf("%T", (*execpb.Tree)(nil)))
		key.Metadata.Put("path", path)
		return job.c.UploadBytes(ctx, key, treebytes)
	})

	return digest, nil
}

func (job *RunningJob) TreeUploadDirectory(ctx context.Context, alldirs *[]*execpb.Directory, allbytes *[]*[]byte, path string) (*execpb.Digest, error) {
	var dir execpb.Directory
	dirbytes := new([]byte)
	*alldirs = append(*alldirs, &dir)
	*allbytes = append(*allbytes, dirbytes)

	entries, err := os.ReadDir(filepath.Join(job.ExecRoot, path))
	if err != nil {
		return nil, err
	}

	eg, _ := errgroup.WithContext(ctx)

	for _, entry := range entries {
		entrypath := filepath.Join(path, entry.Name())
		if entry.Type()&fs.ModeSymlink != 0 {
			target, err := os.Readlink(entrypath)
			if err != nil {
				return nil, fmt.Errorf("Cannot readlink %q: %v", entrypath, err)
			}

			stat, _ := os.Lstat(path)

			dir.Symlinks = append(dir.Symlinks, &execpb.SymlinkNode{
				Name:           entry.Name(),
				Target:         target,
				NodeProperties: statToNodeProperties(stat),
			})
		} else if entry.IsDir() {
			dirnode := &execpb.DirectoryNode{
				Name: entry.Name(),
				// Digest: digest,
			}
			dir.Directories = append(dir.Directories, dirnode)
			eg.Go(func() error {
				digest, err := job.TreeUploadDirectory(ctx, alldirs, allbytes, entrypath)
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
				digest, stat, err := job.UploadFile(ctx, entrypath, eg)
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

func (job *RunningJob) UploadFile(ctx context.Context, path string, eg *errgroup.Group) (*execpb.Digest, fs.FileInfo, error) {
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
	size := stat.Size()

	rawhash := h.Sum(nil)
	hash := fmt.Sprintf("%x", rawhash)
	digest := &execpb.Digest{
		Hash:      hash,
		SizeBytes: size,
	}

	// zero size files don't need to be uploaded
	if size == 0 {
		return digest, stat, nil
	}

	key := storage.BlobKey{InstanceName: job.InstanceName, Kind: storage.CONTENT_CAS, Digest: hash, Size: int(size), ExpiresMin: 12 * time.Hour}
	key.Metadata.Put("B-Type", "File")
	key.Metadata.Put("B-Path", path)
	key.Metadata.Put("B-OutputOf", job.Id)

	// move to cache so future jobs can reuse it without downloading
	if err := os.Rename(filepath.Join(job.ExecRoot, path), job.c.CachePath(key)); err != nil {
		return nil, nil, err
	}

	if _, err = f.Seek(0, 0); err != nil {
		return nil, nil, err
	}

	eg.Go(func() error {
		defer f.Close()

		start := time.Now()

		eg, ctx := errgroup.WithContext(ctx)

		// check if it exists, race with uploading
		eg.Go(func() error {
			_, err := job.c.Exists(ctx, key)
			if err == nil {
				return os.ErrExist
			}
			return nil
		})

		eg.Go(func() error {
			var err error
			w, err := job.c.UploadWriter(ctx, key)
			if err != nil {
				return err
			}
			defer w.Close()
			if _, err := io.Copy(w, f); err != nil {
				return err
			}
			if err := w.Finish(); err != nil {
				return err
			}
			return nil
		})

		if err := eg.Wait(); err != nil {
			if err == os.ErrExist {
				return nil
			}
		}

		uploadDuration := time.Since(start)
		if uploadDuration > 1*time.Second {
			logrus.Printf("UploadFile  %.1f MB took %v (%.1f MB/s)  %v", utils.MB(int(size)), uploadDuration, utils.MBPS(int(size), uploadDuration), path)
		}
		return nil
	})

	return digest, stat, nil
}
