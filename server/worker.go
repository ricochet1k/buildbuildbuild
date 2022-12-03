package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Server) InitWorker() {
	c.jobSlots = make(chan struct{}, c.config.WorkerSlots)
	go c.PeriodicSendState()
}

func (c *Server) PeriodicSendState() {
	time.Sleep(1 * time.Second)

	lastMemberCount := 0
	running := 0
	slotsFree := 0
	lastSend := time.Now()
	for {
		runningNow := len(c.jobSlots)
		freeNow := cap(c.jobSlots) - runningNow
		memberCount := len(c.list.Members())
		if time.Since(lastSend) > 20*time.Second || lastMemberCount != memberCount || runningNow != running || freeNow != slotsFree {
			lastMemberCount = memberCount
			running = runningNow
			slotsFree = freeNow
			lastSend = time.Now()
			c.SendState(&clusterpb.NodeState{
				JobsRunning:   int32(running),
				JobsSlotsFree: int32(slotsFree),
			})
		}

		time.Sleep(1 * time.Second)
	}
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
	Id                string
	InstanceName      string
	ExecRoot          string
	ActionDigest      *execpb.Digest
	inputBytes        int64
	downloadedBytes   int64
	c                 *Server
	eg                *errgroup.Group
	missingBlobsMutex sync.Mutex
	missingBlobs      []*execpb.Digest
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

	logrus.Infof("Starting job %v %v (%v)", req.RequestMetadata.ActionMnemonic, req.RequestMetadata.TargetId, req.RequestMetadata.ToolInvocationId)

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

	// logrus.Infof("Worker starting job: %v  in %q  %v\n", req.Id, execroot, req.ActionDigest)
	// defer logrus.Infof("Worker job complete: %v\n", req.Id)

	// we aren't using egctx here because we want to give the client the full list of missing blobs if there are any, rather than cutting them short after the first one
	eg, _ := errgroup.WithContext(ctx)
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
	actionKey := storage.BlobKey{InstanceName: req.InstanceName, Kind: storage.CONTENT_CAS, Digest: req.ActionDigest.Hash, Size: int(req.ActionDigest.SizeBytes)}
	if err := c.DownloadProto(ctx, actionKey, &action); err != nil {
		log.Printf("Worker cannot get action %v: %v", actionKey, err)

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
	commandKey := storage.BlobKey{InstanceName: req.InstanceName, Kind: storage.CONTENT_CAS, Digest: action.CommandDigest.Hash, Size: int(action.CommandDigest.SizeBytes)}
	eg.Go(func() error {
		err := c.DownloadProto(ctx, commandKey, &command)
		if err != nil {
			job.AddMissing(action.CommandDigest)
		}

		return err
	})

	start := time.Now()
	job.UpdateJobStatus(&clusterpb.JobStatus{
		Stage: execpb.ExecutionStage_CACHE_CHECK,
	})
	metadata.InputFetchStartTimestamp = timestamppb.Now()
	eg.Go(func() error { return job.DownloadAll(ctx, "", action.InputRootDigest) })

	waitingForDownload := make(chan struct{})
	go func() {
		for {
			select {
			case <-time.NewTimer(5 * time.Second).C:
			case <-waitingForDownload:
				return
			}

			downloadDuration := time.Since(start)
			downloadedBytes := int(atomic.LoadInt64(&job.downloadedBytes))
			inputBytes := int(atomic.LoadInt64(&job.inputBytes))
			logrus.Printf("Downloading %.1f/%.1f MB for %.1f...", utils.MB(downloadedBytes), utils.MB(inputBytes), downloadDuration.Seconds())
		}
	}()

	err = job.eg.Wait()
	close(waitingForDownload)
	metadata.InputFetchCompletedTimestamp = timestamppb.Now()
	if err != nil {
		logrus.Errorf("Worker cannot get inputRoot: %v", err)
		if len(job.missingBlobs) > 0 {
			logrus.Errorf("MissingBlobs: %v", job.missingBlobs)
		}

		jobStatus := &clusterpb.JobStatus{
			Stage:         execpb.ExecutionStage_UNKNOWN,
			Error:         status.New(codes.FailedPrecondition, fmt.Sprintf("Input fetch: %v", err)).Proto(),
			MissingDigest: job.missingBlobs,
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
	// put subprocesses in a new group
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = filepath.Join(execroot, command.WorkingDirectory)
	for _, envvar := range command.EnvironmentVariables {
		cmd.Env = append(cmd.Env, envvar.Name+"="+envvar.Value)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// logrus.Printf("Running: %v in %v", cmd.Args, cmd.Dir)
	metadata.ExecutionStartTimestamp = timestamppb.Now()
	err = cmd.Run()
	metadata.ExecutionCompletedTimestamp = timestamppb.Now()
	// logrus.Printf("Running Complete! %v", err)

	// If there are any left-over processes, kill them.
	// This works in tandem with Setpgid above, which puts them into a
	// new process group numbered by the pid, which kill can target if it is negative.
	syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)

	actionResult := &execpb.ActionResult{
		ExecutionMetadata: metadata,
	}
	if err != nil {
		logrus.Printf("Job Execute error: %q in %v: %v", cmd.Args, cmd.Dir, err)

		var exiterr *exec.ExitError
		if errors.As(err, &exiterr) {
			actionResult.ExitCode = int32(exiterr.ExitCode())
		}
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
				logrus.Error("Upload Output path %q: %v", path, err)
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
				logrus.Errorf("Upload Output file %q: %v", path, err)
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
				logrus.Errorf("Upload Output dir %q: %v", path, err)
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

	// logrus.Printf("actionResult: %#v", actionResult)
	// for _, outfile := range actionResult.OutputFiles {
	// 	logrus.Printf("outfile: %#v %v", outfile, outfile.Digest)
	// }

	metadata.WorkerCompletedTimestamp = timestamppb.Now()

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

	downloadedBytes := int(atomic.LoadInt64(&job.downloadedBytes))
	downloadDur := metadata.InputFetchCompletedTimestamp.AsTime().Sub(metadata.InputFetchStartTimestamp.AsTime())
	executeDur := metadata.ExecutionCompletedTimestamp.AsTime().Sub(metadata.ExecutionStartTimestamp.AsTime())
	uploadDur := metadata.OutputUploadCompletedTimestamp.AsTime().Sub(metadata.OutputUploadStartTimestamp.AsTime())
	totalDur := metadata.WorkerCompletedTimestamp.AsTime().Sub(metadata.WorkerStartTimestamp.AsTime())
	logrus.Infof("Worker job breakdown: %v %v : %.1f MB / %v download, %v execute, %v upload, %v total (%v unknown)", req.RequestMetadata.ActionMnemonic, req.RequestMetadata.TargetId, utils.MB(downloadedBytes), downloadDur, executeDur, uploadDur, totalDur, totalDur-(downloadDur+executeDur+uploadDur))

	return nil
}
