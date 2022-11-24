package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/anypb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *Server) QueueJob(f func(string)) {
	c.jobQueueMutex.Lock()
	defer c.jobQueueMutex.Unlock()
	c.jobQueue = append(c.jobQueue, f)
}

func (c *Server) RequestJob(node string) {
	c.jobQueueMutex.Lock()
	defer c.jobQueueMutex.Unlock()
	if len(c.jobQueue) > 0 {
		job := c.jobQueue[len(c.jobQueue)-1]
		c.jobQueue = c.jobQueue[:len(c.jobQueue)-1]
		job(node)
	}
}

func jobStatusToOperation(jobId string, msg *clusterpb.JobStatus) *longrunningpb.Operation {
	logrus.Printf("job status! %v\n", msg)

	metadata, _ := anypb.New(&execpb.ExecuteOperationMetadata{
		Stage:        msg.Stage,
		ActionDigest: msg.ActionDigest,
		// StdoutStreamName: "",
		// StderrStreamName: "",
	})

	if len(msg.MissingDigest) > 0 {
		violations := []*errdetails.PreconditionFailure_Violation{}

		for _, digest := range msg.MissingDigest {
			violations = append(violations, &errdetails.PreconditionFailure_Violation{
				Type:    "MISSING",
				Subject: fmt.Sprintf("blobs/%v/%v", digest.Hash, digest.SizeBytes),
			})
		}

		logrus.Warnf("Job status missing digests: %v", violations)

		pf, _ := anypb.New(&errdetails.PreconditionFailure{
			Violations: violations,
		})
		msg.Error.Details = append(msg.Error.Details, pf)
	}

	op := &longrunningpb.Operation{
		Name:     jobId,
		Metadata: metadata,
	}

	if msg.Error != nil {
		op.Result = &longrunningpb.Operation_Error{
			Error: msg.Error,
		}
		op.Done = true
	} else if msg.Response != nil {
		response, _ := anypb.New(msg.Response)
		op.Result = &longrunningpb.Operation_Response{
			Response: response,
		}
		op.Done = true
	}

	return op
}

// Execute an action remotely.
//
// In order to execute an action, the client must first upload all of the
// inputs, the
// [Command][build.bazel.remote.execution.v2.Command] to run, and the
// [Action][build.bazel.remote.execution.v2.Action] into the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
// It then calls `Execute` with an `action_digest` referring to them. The
// server will run the action and eventually return the result.
//
// The input `Action`'s fields MUST meet the various canonicalization
// requirements specified in the documentation for their types so that it has
// the same digest as other logically equivalent `Action`s. The server MAY
// enforce the requirements and return errors if a non-canonical input is
// received. It MAY also proceed without verifying some or all of the
// requirements, such as for performance reasons. If the server does not
// verify the requirement, then it will treat the `Action` as distinct from
// another logically equivalent action if they hash differently.
//
// Returns a stream of
// [google.longrunning.Operation][google.longrunning.Operation] messages
// describing the resulting execution, with eventual `response`
// [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
// `metadata` on the operation is of type
// [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
//
// If the client remains connected after the first response is returned after
// the server, then updates are streamed as if the client had called
// [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
// until the execution completes or the request reaches an error. The
// operation can also be queried using [Operations
// API][google.longrunning.Operations.GetOperation].
//
// The server NEED NOT implement other methods or functionality of the
// Operations API.
//
// Errors discovered during creation of the `Operation` will be reported
// as gRPC Status errors, while errors that occurred while running the
// action will be reported in the `status` field of the `ExecuteResponse`. The
// server MUST NOT set the `error` field of the `Operation` proto.
// The possible errors include:
//
//   - `INVALID_ARGUMENT`: One or more arguments are invalid.
//   - `FAILED_PRECONDITION`: One or more errors occurred in setting up the
//     action requested, such as a missing input or command or no worker being
//     available. The client may be able to fix the errors and retry.
//   - `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
//     the action.
//   - `UNAVAILABLE`: Due to a transient condition, such as all workers being
//     occupied (and the server does not support a queue), the action could not
//     be started. The client should retry.
//   - `INTERNAL`: An internal error occurred in the execution engine or the
//     worker.
//   - `DEADLINE_EXCEEDED`: The execution timed out.
//   - `CANCELLED`: The operation was cancelled by the client. This status is
//     only possible if the server implements the Operations API CancelOperation
//     method, and it was called for the current execution.
//
// In the case of a missing input or command, the server SHOULD additionally
// send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
// where, for each requested blob not present in the CAS, there is a
// `Violation` with a `type` of `MISSING` and a `subject` of
// `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
//
// The server does not need to guarantee that a call to this method leads to
// at most one execution of the action. The server MAY execute the action
// multiple times, potentially in parallel. These redundant executions MAY
// continue to run, even if the operation is completed.
func (c *Server) Execute(req *execpb.ExecuteRequest, es execpb.Execution_ExecuteServer) error {
	job := &clusterpb.StartJobRequest{
		Id:              uuid.New().String(),
		InstanceName:    req.InstanceName,
		ActionDigest:    req.ActionDigest,
		SkipCacheLookup: req.SkipCacheLookup,
		QueuedTimestamp: timestamppb.Now(),
	}
	completed := make(chan struct{})

	handleJobStatus := func(status *clusterpb.JobStatus) {
		op := jobStatusToOperation(job.Id, status)

		err := es.Send(op)
		if err != nil {
			logrus.Printf("Error sending longrunning.Operation: %v", err)
		}
	}

	startJob := func(node string) error {
		logrus.Printf("Sending job to %v: %v\n", node, job.Id)

		conn, err := c.ConnectToMember(node)
		if err == nil {
			worker := clusterpb.NewWorkerClient(conn)
			stream, err := worker.StartJob(es.Context(), job)
			if err != nil {
				logrus.Printf("Error starting job %v: %v\n", node, job.Id)
				return err
			}

			// read first message to see if it actually started
			status, err := stream.Recv()
			if err != nil {
				logrus.Printf("Received initial err from JobStatus: %v", err)
				return err
			}

			go func() {
				for {
					handleJobStatus(status)

					status, err = stream.Recv()
					if err != nil {
						if !errors.Is(err, io.EOF) {
							logrus.Printf("Received err from JobStatus: %v", err)
						}
						break
					}

				}
				close(completed)
			}()
			return nil
		}
		return fmt.Errorf("Could send job to %v: %w\n", node, err)
	}

	var startJobQueued func(node string)
	startJobQueued = func(node string) {
		err := startJob(node)
		if err == nil {
			return
		}

		logrus.Printf("Queueing job: %v\n", job.Id)
		c.QueueJob(startJobQueued)
	}

	started := false
	for node, state := range c.nodeState {
		if state.JobsSlotsFree > 0 {
			err := startJob(node)
			if err == nil {
				started = true
				break
			}
		}
	}
	if !started {
		logrus.Printf("Queueing job: %v\n", job.Id)
		handleJobStatus(&clusterpb.JobStatus{
			Stage: execpb.ExecutionStage_QUEUED,
		})
		c.QueueJob(startJobQueued)
	}

	<-completed

	return nil // status.Error(codes.Unimplemented, "Execute not implemented")
}

// Wait for an execution operation to complete. When the client initially
// makes the request, the server immediately responds with the current status
// of the execution. The server will leave the request stream open until the
// operation completes, and then respond with the completed operation. The
// server MAY choose to stream additional updates as execution progresses,
// such as to provide an update as to the state of the execution.
func (c *Server) WaitExecution(req *execpb.WaitExecutionRequest, wes execpb.Execution_WaitExecutionServer) error {
	// Find where job is running with a Query
	query, err := c.list.Query("wherejob", []byte(req.Name), &serf.QueryParam{
		FilterNodes: nil,
		FilterTags:  map[string]string{"worker": "true"},
		RequestAck:  true,
		RelayFactor: 1,
		Timeout:     time.Second,
	})
	if err != nil {
		logrus.Printf("Query error: %v", err)
		return err
	}

	if acks := query.AckCh(); acks != nil {
		go func() {
			for ack := range acks {
				logrus.Printf("ACK from: %v", ack)
			}
		}()
	}
	var node string
	for response := range query.ResponseCh() {
		logrus.Printf("Response: %v", response)
		node = string(response.From)
	}

	// Connect and FollowJob
	if node == "" {
		return status.Error(codes.NotFound, "Job not found")
	}

	conn, err := c.ConnectToMember(node)
	if err != nil {
		return status.Errorf(codes.NotFound, "Unable to connect to worker: %v", err)
	}

	worker := clusterpb.NewWorkerClient(conn)
	stream, err := worker.FollowJob(wes.Context(), &clusterpb.FollowJobRequest{Id: req.Name})
	if err != nil {
		return status.Errorf(codes.NotFound, "Unable to follow job: %v", err)
	}

	for {
		status, err := stream.Recv()
		if err != nil {
			logrus.Printf("Received err from JobStatus: %v", err)
			break
		}

		logrus.Printf("job status! %v\n", status)

		op := jobStatusToOperation(req.Name, status)

		err = wes.Send(op)
		if err != nil {
			logrus.Printf("Error sending longrunning.Operation: %v", err)
			break
		}
	}

	return nil
}

// CancelOperation implements longrunningpb.OperationsServer
func (*Server) CancelOperation(ctx context.Context, req *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	panic("unimplemented")
}

// DeleteOperation implements longrunningpb.OperationsServer
func (*Server) DeleteOperation(ctx context.Context, req *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	panic("unimplemented")
}

// GetOperation implements longrunningpb.OperationsServer
func (*Server) GetOperation(ctx context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	panic("unimplemented")
}

// ListOperations implements longrunningpb.OperationsServer
func (*Server) ListOperations(ctx context.Context, req *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	panic("unimplemented")
}

// WaitOperation implements longrunningpb.OperationsServer
func (*Server) WaitOperation(ctx context.Context, req *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	panic("unimplemented")
}
