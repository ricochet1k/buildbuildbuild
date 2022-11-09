package server

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/anypb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
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
// * `INVALID_ARGUMENT`: One or more arguments are invalid.
// * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
//   action requested, such as a missing input or command or no worker being
//   available. The client may be able to fix the errors and retry.
// * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
//   the action.
// * `UNAVAILABLE`: Due to a transient condition, such as all workers being
//   occupied (and the server does not support a queue), the action could not
//   be started. The client should retry.
// * `INTERNAL`: An internal error occurred in the execution engine or the
//   worker.
// * `DEADLINE_EXCEEDED`: The execution timed out.
// * `CANCELLED`: The operation was cancelled by the client. This status is
//   only possible if the server implements the Operations API CancelOperation
//   method, and it was called for the current execution.
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
	job := &clusterpb.StartJob{
		Id:              uuid.New().String(),
		InstanceName:    req.InstanceName,
		ActionDigest:    req.ActionDigest,
		SkipCacheLookup: req.SkipCacheLookup,
	}
	running := false
	completed := make(chan struct{})

	var startJob func(node string)
	startJob = func(node string) {
		if running {
			return
		}
		fmt.Printf("Sending job to %v: %v\n", node, job.Id)
		c.SendNodeMessage(node, &clusterpb.NodeMessage{StartJob: job})
		time.Sleep(3 * time.Second)
		if !running {
			fmt.Printf("Queueing job: %v\n", job.Id)
			c.QueueJob(startJob)
		}
	}

	c.SubscribeFn("jobstatus:"+job.Id, func(c *Server, key string, msg *clusterpb.Message) bool {
		fmt.Printf("job status! %v\n", msg)
		running = true

		metadata, _ := anypb.New(&execpb.ExecuteOperationMetadata{
			Stage:        msg.GetJobStatus().Stage,
			ActionDigest: req.ActionDigest,
			// StdoutStreamName: "",
			// StderrStreamName: "",
		})

		es.Send(&longrunningpb.Operation{
			Name:     job.Id,
			Metadata: metadata,
		})
		if msg.GetJobStatus().Stage == execpb.ExecutionStage_COMPLETED {
			close(completed)
			return false // stop listening
		}
		return true // keep listening
	})

	started := false
	for node, state := range c.nodeState {
		if state.JobsSlotsFree > 0 {
			startJob(node)
			started = true
			break
		}
	}
	if !started {
		fmt.Printf("Queueing job: %v\n", job.Id)
		c.QueueJob(startJob)
	}

	metadata, _ := anypb.New(&execpb.ExecuteOperationMetadata{
		Stage:        execpb.ExecutionStage_QUEUED,
		ActionDigest: req.ActionDigest,
		// StdoutStreamName: "",
		// StderrStreamName: "",
	})

	es.Send(&longrunningpb.Operation{
		Name:     job.Id,
		Metadata: metadata,
	})

	<-completed

	return status.Error(codes.Unimplemented, "Execute not implemented")
}

// Wait for an execution operation to complete. When the client initially
// makes the request, the server immediately responds with the current status
// of the execution. The server will leave the request stream open until the
// operation completes, and then respond with the completed operation. The
// server MAY choose to stream additional updates as execution progresses,
// such as to provide an update as to the state of the execution.
func (c *Server) WaitExecution(req *execpb.WaitExecutionRequest, wes execpb.Execution_WaitExecutionServer) error {
	fmt.Println("Unimplemented: Execute")
	return status.Error(codes.Unimplemented, "WaitExecution not implemented")
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
