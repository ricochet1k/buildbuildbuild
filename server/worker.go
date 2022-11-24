package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Server) InitWorker() {
	c.jobSlots = make(chan struct{}, c.config.WorkerSlots)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.SendState(&clusterpb.NodeState{
				JobsRunning:   int32(len(c.jobSlots)),
				JobsSlotsFree: int32(cap(c.jobSlots)) - int32(len(c.jobSlots)),
			})
		}
	}()

	// periodically clean the cache to keep the disk from filling up
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			c.CleanOldCacheFiles()
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
	inputBytes      int64
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

func (c *Server) StartJob(req *clusterpb.StartJobRequest, stream clusterpb.Worker_StartJobServer) (finalerr error) {
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

	if c.config.NoCleanupExecroot != "all" {
		defer func() {
			if c.config.NoCleanupExecroot != "fail" || finalerr == nil {
				go func() {
					if err := os.RemoveAll(execroot); err != nil {
						logrus.Warnf("Unable to remove execroot %q: %v", execroot, err)
					}
				}()
			}
		}()
	}

	fmt.Printf("Worker starting job: %v  in %q  %v\n", req.Id, execroot, req.ActionDigest)
	defer fmt.Printf("Worker job complete: %v\n", req.Id)

	eg, egctx := errgroup.WithContext(ctx)
	// eg.SetLimit(c.config.DownloadConcurrency)
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
			Stage:         execpb.ExecutionStage_UNKNOWN,
			Error:         status.New(codes.FailedPrecondition, fmt.Sprintf("Missing action %v", req.ActionDigest)).Proto(),
			MissingDigest: []*execpb.Digest{req.ActionDigest},
		})
		return err
	}

	if action.Platform != nil && len(action.Platform.Properties) > 0 {
		logrus.Printf("Action platform: %v", action.Platform)
	}

	var command execpb.Command
	eg.Go(func() error {
		err := c.DownloadProto(ctx, StorageKey(req.InstanceName, CONTENT_CAS, DigestKey(action.CommandDigest)), &command)

		if err != nil {
			err = WrapErrorWithMissing(err, action.CommandDigest)
		}

		return err
	})

	// start := time.Now()
	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_CACHE_CHECK,
	})
	metadata.InputFetchStartTimestamp = timestamppb.Now()
	eg.Go(func() error { return job.DownloadAll(egctx, "", action.InputRootDigest) })

	// waitingForDownload := make(chan struct{})
	// go func() {
	// 	done := false
	// 	for !done {
	// 		select {
	// 		case <-time.NewTimer(5 * time.Second).C:
	// 		case <-waitingForDownload:
	// 			done = true
	// 		}

	// 		downloadDuration := time.Since(start)
	// 		mbs := float64(atomic.LoadInt64(&job.downloadedBytes)) / 1000000.0
	// 		mbstotal := float64(atomic.LoadInt64(&job.inputBytes)) / 1000000.0
	// 		logrus.Printf("Downloading %.1f/%.1f MB for %.1f... (maybe waiting for others)", mbs, mbstotal, downloadDuration)
	// 	}
	// }()

	err = job.eg.Wait()
	// close(waitingForDownload)
	metadata.InputFetchCompletedTimestamp = timestamppb.Now()
	if err != nil {
		logrus.Printf("Worker cannot get inputRoot: %v", err)

		jobStatus := &clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_UNKNOWN,
			Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Input fetch: %v", err)).Proto(),
		}

		var withMissing ErrorWithMissingDigests
		if errors.As(err, &withMissing) {
			jobStatus.MissingDigest = withMissing.MissingDigests
		}

		job.UpdateJobStatus(jobStatus)
		return nil
	}

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
				return nil
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
				return nil
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
				return nil
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
	logrus.Printf("Running Complete! %v", err)

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
		return nil
	}

	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_COMPLETED,
	})

	// upload outputs
	metadata.OutputUploadStartTimestamp = timestamppb.Now()
	if len(command.OutputPaths) > 0 {
		for _, path := range command.OutputPaths {
			if _, err := job.UploadOutputPath(ctx, actionResult, path); err != nil {
				logrus.Error(err)
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, err.Error()).Proto(),
				})
				return nil
			}
		}
	} else {
		// OutputFiles/Directories instead, supposedly "deprecated" but still used by Bazel
		for _, path := range command.OutputFiles {
			isdir, err := job.UploadOutputPath(ctx, actionResult, path)
			if err != nil {
				logrus.Error(err)
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, err.Error()).Proto(),
				})
				return nil
			}

			if isdir {
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("OutputFile is a directory: %v", path)).Proto(),
				})
				return nil
			}
		}

		for _, path := range command.OutputDirectories {
			isdir, err := job.UploadOutputPath(ctx, actionResult, path)
			if err != nil {
				logrus.Error(err)
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, err.Error()).Proto(),
				})
				return nil
			}

			if !isdir {
				job.UpdateJobStatus(&clusterpb.JobStatus{
					Stage: execpb.ExecutionStage_COMPLETED,
					Error: status.New(codes.FailedPrecondition, fmt.Sprintf("OutputDirectory is a file: %v", path)).Proto(),
				})
				return nil
			}
		}
	}

	if err = eg.Wait(); err != nil {
		logrus.Printf("Worker cannot upload: %v", err)

		job.UpdateJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_COMPLETED,
			Error: status.New(codes.FailedPrecondition, fmt.Sprintf("Upload failed: %v", err)).Proto(),
		})
		return nil
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

	return nil
}
