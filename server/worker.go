package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	wrapperspb "github.com/golang/protobuf/ptypes/wrappers"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Server) InitWorker() {
	c.jobSlots = make(chan struct{}, 4)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.SendState(&clusterpb.NodeState{
				JobsRunning:   int32(len(c.jobSlots)),
				JobsSlotsFree: int32(cap(c.jobSlots)) - int32(len(c.jobSlots)),
			})
		}
	}()
}

func (c *Server) DownloadProto(ctx context.Context, key string, msg proto.Message) error {
	var b aws.WriteAtBuffer
	_, err := c.downloader.DownloadWithContext(ctx, &b, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b.Bytes(), msg); err != nil {
		return err
	}

	return nil
}

// FollowJob implements clusterpb.WorkerServer
func (c *Server) FollowJob(req *clusterpb.FollowJobRequest, stream clusterpb.Worker_FollowJobServer) error {

	jobStatus, ok := c.jobsRunning.Load(req.Id)
	if !ok {
		return status.Error(codes.NotFound, "job not found")
	}

	if jobStatus != nil {
		jobStatus := jobStatus.(*clusterpb.JobStatus)
		err := stream.Send(jobStatus)
		if err != nil {
			return err
		}
		if jobStatus.Response != nil {
			logrus.Printf("FollowJob of completed job: %v", jobStatus)
			// response is set when job is complete, no point in listening
			return nil
		}
	}

	completed := make(chan struct{})
	c.SubscribeFn(req.Id, func(c *Server, key string, msg *clusterpb.Message) bool {
		err := stream.Send(msg.GetJobStatus())
		if msg.GetJobStatus().Response != nil {
			close(completed)
			return false // stop listening
		}
		return err == nil // keep if still connected
	})
	<-completed

	return nil
}

type RunningJob struct {
	Id              string
	InstanceName    string
	ExecRoot        string
	ActionDigest    *execpb.Digest
	downloadedBytes int64
	c               *Server
	eg              *errgroup.Group
}

func (j *RunningJob) UpdateJobStatus(status *clusterpb.JobStatus) {
	status.ActionDigest = j.ActionDigest
	j.c.Publish(j.Id, &clusterpb.Message{
		Type: &clusterpb.Message_JobStatus{JobStatus: status},
	})

	j.c.jobsRunning.Store(j.Id, status)
}

func (c *Server) StartJob(req *clusterpb.StartJobRequest, stream clusterpb.Worker_StartJobServer) error {
	select {
	case c.jobSlots <- struct{}{}:
		defer func() { <-c.jobSlots }()
	default:
		return status.Error(codes.ResourceExhausted, "Cannot start job, slots full")
	}

	metadata := &execpb.ExecutedActionMetadata{
		Worker:          c.list.LocalMember().Name,
		QueuedTimestamp: req.QueuedTimestamp,
	}

	metadata.WorkerStartTimestamp = timestamppb.Now()

	// we don't want to stop the job when the stream disconnects
	ctx := context.TODO()

	c.jobsRunning.Store(req.Id, nil)
	defer func() {
		go func() {
			time.Sleep(10 * time.Second)

			c.jobsRunning.Delete(req.Id)
		}()
	}()

	execroot, err := os.MkdirTemp("", req.Id)
	if err != nil {
		logrus.Errorf("MkdirTemp: %v", err)
		return err
	}

	fmt.Printf("Worker starting job: %v  in %q  %v\n", req.Id, execroot, req.ActionDigest)
	defer fmt.Printf("Worker job complete: %v\n", req.Id)

	eg, egctx := errgroup.WithContext(ctx)
	job := &RunningJob{
		Id:           req.Id,
		InstanceName: req.InstanceName,
		ExecRoot:     execroot,
		ActionDigest: req.ActionDigest,
		c:            c,
		eg:           eg,
	}

	c.SubscribeFn(req.Id, func(c *Server, key string, msg *clusterpb.Message) bool {
		err := stream.Send(msg.GetJobStatus())
		return err == nil // keep if still connected
	})

	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_CACHE_CHECK,
	})

	var action execpb.Action
	if err := c.DownloadProto(ctx, StorageKey(req.InstanceName, CONTENT_CAS, DigestKey(req.ActionDigest)), &action); err != nil {
		log.Printf("Worker cannot get action: %v", err)

		job.UpdateJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_COMPLETED,
			Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Missing action %v", req.ActionDigest)).Proto(),
		})
		return err
	}

	if action.Platform != nil && len(action.Platform.Properties) > 0 {
		logrus.Printf("Action platform: %v", action.Platform)
	}

	start := time.Now()
	metadata.InputFetchStartTimestamp = timestamppb.Now()
	eg.Go(func() error { return c.DownloadAll(egctx, job, "", action.InputRootDigest) })

	var command execpb.Command
	if err := c.DownloadProto(ctx, StorageKey(req.InstanceName, CONTENT_CAS, DigestKey(action.CommandDigest)), &command); err != nil {
		logrus.Printf("Worker cannot get command: %v", err)

		job.UpdateJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_COMPLETED,
			Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Missing command %v", action.CommandDigest)).Proto(),
		})
		return err
	}

	err = job.eg.Wait()
	metadata.InputFetchCompletedTimestamp = timestamppb.Now()
	if err != nil {
		logrus.Printf("Worker cannot get inputRoot: %v", err)

		job.UpdateJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_COMPLETED,
			Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Input fetch: %v", err)).Proto(),
		})
		return err
	}
	downloadDuration := time.Since(start)
	mbs := float64(atomic.LoadInt64(&job.downloadedBytes)) / 1000000.0
	logrus.Printf("Downloaded %.1f MB in %v (%.1f MB/s)", mbs, downloadDuration, mbs/downloadDuration.Seconds())

	// make sure all output path dirs exist
	if len(command.OutputPaths) > 0 {
		for _, path := range command.OutputPaths {
			err := os.MkdirAll(filepath.Join(execroot, filepath.Dir(path)), os.FileMode(0755))
			if err != nil && !os.IsExist(err) {
				logrus.Printf("Worker cannot mkdir output dir %q: %v", path, err)

				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_UNKNOWN,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Cannot mkdir output dir %q: %v", path, err)).Proto(),
				})
				return err
			}
		}
	} else {
		for _, path := range command.OutputFiles {
			err := os.MkdirAll(filepath.Join(execroot, filepath.Dir(path)), os.FileMode(0755))
			if err != nil && !os.IsExist(err) {
				logrus.Printf("Worker cannot mkdir output dir %q: %v", path, err)

				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_UNKNOWN,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Cannot mkdir output dir %q: %v", path, err)).Proto(),
				})
				return err
			}
		}

		for _, path := range command.OutputDirectories {
			err := os.MkdirAll(filepath.Join(execroot, path), os.FileMode(0755))
			if err != nil && !os.IsExist(err) {
				logrus.Printf("Worker cannot mkdir output dir %q: %v", path, err)

				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_UNKNOWN,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Cannot mkdir output dir %q: %v", path, err)).Proto(),
				})
				return err
			}
		}
	}

	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_EXECUTING,
	})

	cmdctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	cmd := exec.CommandContext(cmdctx, command.Arguments[0], command.Arguments[1:]...)
	cmd.Dir = filepath.Join(execroot, command.WorkingDirectory)
	for _, envvar := range command.EnvironmentVariables {
		cmd.Env = append(cmd.Env, envvar.Name+"="+envvar.Value)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logrus.Printf("Running: %v in %v", cmd.Args, cmd.Dir)
	metadata.ExecutionStartTimestamp = timestamppb.Now()
	err = cmd.Run()
	metadata.ExecutionCompletedTimestamp = timestamppb.Now()
	logrus.Printf("Running Complete!")

	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_COMPLETED,
	})

	actionResult := &execpb.ActionResult{
		ExecutionMetadata: metadata,
	}
	if err != nil {
		job.UpdateJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_COMPLETED,
			Response: &execpb.ExecuteResponse{
				Result:       actionResult,
				CachedResult: false,
				Status:       status.Convert(err).Proto(),
				// ServerLogs:   map[string]*execpb.LogFile{},
				Message: "Command execution failed",
			},
		})
	}

	// upload outputs
	metadata.OutputUploadStartTimestamp = timestamppb.Now()
	if len(command.OutputPaths) > 0 {
		for _, path := range command.OutputPaths {
			if _, err := c.UploadOutputPath(ctx, job, actionResult, path); err != nil {
				logrus.Error(err)
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, err.Error()).Proto(),
				})
				return err
			}
		}
	} else {
		// OutputFiles/Directories instead, supposedly "deprecated" but still used by Bazel
		for _, path := range command.OutputFiles {
			isdir, err := c.UploadOutputPath(ctx, job, actionResult, path)
			if err != nil {
				logrus.Error(err)
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, err.Error()).Proto(),
				})
				return err
			}

			if isdir {
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("OutputFile is a directory: %v", path)).Proto(),
				})
				return err
			}
		}

		for _, path := range command.OutputDirectories {
			isdir, err := c.UploadOutputPath(ctx, job, actionResult, path)
			if err != nil {
				logrus.Error(err)
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, err.Error()).Proto(),
				})
				return err
			}

			if !isdir {
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("OutputDirectory is a file: %v", path)).Proto(),
				})
				return err
			}
		}
	}

	if err = eg.Wait(); err != nil {
		logrus.Printf("Worker cannot upload: %v", err)

		job.UpdateJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_COMPLETED,
			Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Upload failed: %v", err)).Proto(),
		})
		return err
	}
	metadata.OutputUploadCompletedTimestamp = timestamppb.Now()

	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_COMPLETED,
		Response: &execpb.ExecuteResponse{
			Result:       actionResult,
			CachedResult: false,
			Status:       status.New(codes.OK, "done").Proto(),
			// ServerLogs:   map[string]*execpb.LogFile{},
			Message: "This is a Message",
		},
	})

	// TODO: debug flag to keep execroot
	go func() {
		err := os.RemoveAll(execroot)
		if err != nil {
			logrus.Warnf("Unable to remove execroot %q: %v", execroot, err)
		}
	}()

	return nil
}

func (c *Server) DownloadAll(ctx context.Context, job *RunningJob, path string, dirDigest *execpb.Digest) error {
	var dir execpb.Directory
	if err := c.DownloadProto(ctx, StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(dirDigest)), &dir); err != nil {
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

	if err := c.ApplyNodeProperties(ctx, job, path, dir.NodeProperties); err != nil {
		return err
	}

	for _, subdir := range dir.Directories {
		subdir := subdir
		job.eg.Go(func() error {
			return c.DownloadAll(ctx, job, filepath.Join(path, subdir.Name), subdir.Digest)
		})
	}

	for _, file := range dir.Files {
		file := file
		job.eg.Go(func() error {
			return c.DownloadFile(ctx, job, filepath.Join(path, file.Name), file)
		})
	}

	for _, symlink := range dir.Symlinks {
		sympath := filepath.Join(path, symlink.Name)
		if err := os.Symlink(filepath.Join(job.ExecRoot, symlink.Target), filepath.Join(job.ExecRoot, sympath)); err != nil {
			return fmt.Errorf("Symlink %q: %w", sympath, err)
		}

		if err := c.ApplyNodeProperties(ctx, job, sympath, symlink.NodeProperties); err != nil {
			return err
		}
	}

	return nil
}

func (c *Server) ApplyNodeProperties(ctx context.Context, job *RunningJob, path string, props *execpb.NodeProperties) error {
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

func (c *Server) DownloadFile(ctx context.Context, job *RunningJob, path string, fileNode *execpb.FileNode) error {
	// logrus.Printf("DownloadFile %v", path)
	// start := time.Now()
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

	if fileNode.Digest.SizeBytes > 0 {
		key := StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(fileNode.Digest))

		downloader := s3manager.NewDownloader(c.sess, func(d *s3manager.Downloader) { d.PartSize = 20 * 1000 * 1000 })
		n, err := downloader.DownloadWithContext(ctx, f, &s3.GetObjectInput{
			Bucket: &c.bucket,
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("Download %q: %w", key, err)
		}
		atomic.AddInt64(&job.downloadedBytes, n)
	}

	if err := c.ApplyNodeProperties(ctx, job, path, fileNode.NodeProperties); err != nil {
		return err
	}

	// dur := time.Since(start)
	// log.Printf("DownloadFile %v (%v) took %v", path, fileNode.Digest.SizeBytes, dur)

	return nil
}

// returns true if path refers to a directory (possibly through a symlink)
func (c *Server) UploadOutputPath(ctx context.Context, job *RunningJob, actionResult *execpb.ActionResult, path string) (bool, error) {
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

		isdir, err := c.UploadOutputPath(ctx, job, actionResult, target)
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
			digest, err := c.UploadTree(ctx, job, path)
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
			digest, stat, err := c.UploadFile(ctx, job, path)
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

func (c *Server) UploadTree(ctx context.Context, job *RunningJob, path string) (*execpb.Digest, error) {
	// We are manually generating this proto in topologically sorted order, partly to avoid the overhead
	// of proto.Marshal multiple times per directory. See execpb.OutputDirectory.IsTopologicallySorted for more detail.
	var alldirs []*execpb.Directory
	var allbytes []*[]byte
	_, err := c.UploadDirectory(ctx, job, &alldirs, &allbytes, path)
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
		_, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Body:           &buf,
			Bucket:         &c.bucket,
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

func (c *Server) UploadDirectory(ctx context.Context, job *RunningJob, alldirs *[]*execpb.Directory, allbytes *[]*[]byte, path string) (*execpb.Digest, error) {
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
				digest, err := c.UploadDirectory(ctx, job, alldirs, allbytes, filepath.Join(path, entry.Name()))
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
				digest, stat, err := c.UploadFile(ctx, job, filepath.Join(path, entry.Name()))
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

func (c *Server) UploadFile(ctx context.Context, job *RunningJob, path string) (*execpb.Digest, fs.FileInfo, error) {
	log.Printf("UploadFile %v", path)
	f, err := os.Open(filepath.Join(job.ExecRoot, path))
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
	hashb64 := base64.StdEncoding.EncodeToString(rawhash)

	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, nil, err
	}

	job.eg.Go(func() error {
		defer f.Close()
		_, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Body:           f,
			Bucket:         &c.bucket,
			ChecksumSHA256: &hashb64,
			Key:            aws.String(StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(digest))),
			Metadata: map[string]*string{
				"B-Type": aws.String("File"),
				"B-Path": &path,
			},
		})
		return err
	})

	return digest, stat, nil
}
