package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	emptypb "google.golang.org/protobuf/types/known/emptypb"

	buildeventpb "google.golang.org/genproto/googleapis/devtools/build/v1"
)

// Publish a build event stating the new state of a build (typically from the
// build queue). The BuildEnqueued event must be publishd before all other
// events for the same build ID.
//
// The backend will persist the event and deliver it to registered frontend
// jobs immediately without batching.
//
// The commit status of the request is reported by the RPC's util_status()
// function. The error code is the canoncial error code defined in
// //util/task/codes.proto.
func (*Server) PublishBuildToolEventStream(stream buildeventpb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Printf("BuildEvent stream err: %v\n", err)
			return err
		}

		err = stream.Send(&buildeventpb.PublishBuildToolEventStreamResponse{
			StreamId:       req.OrderedBuildEvent.StreamId,
			SequenceNumber: req.OrderedBuildEvent.SequenceNumber,
		})
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Printf("BuildEvent stream err: %v\n", err)
			return err
		}

		switch event := req.OrderedBuildEvent.Event.Event.(type) {
		case *buildeventpb.BuildEvent_BazelEvent:
			// fmt.Printf("BuildEvent %q: BazelEvent %v\n", req.ProjectId, event.BazelEvent.TypeUrl)
		default:
			fmt.Printf("BuildEvent %q: unknown %T %v\n", req.ProjectId, event, event)
		}
	}

	return nil
}

// Publish build tool events belonging to the same stream to a backend job
// using bidirectional streaming.
func (*Server) PublishLifecycleEvent(ctx context.Context, req *buildeventpb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	// if md, ok := metadata.FromIncomingContext(ctx); ok {
	// 	fmt.Printf("Metadata: %v\n", md)
	// }

	switch event := req.BuildEvent.Event.Event.(type) {
	case *buildeventpb.BuildEvent_BuildEnqueued_:
		fmt.Printf("PublishLifecycleEvent %q: BuildEnqueued %v\n", req.ProjectId, event.BuildEnqueued.Details)
	case *buildeventpb.BuildEvent_InvocationAttemptStarted_:
		fmt.Printf("PublishLifecycleEvent %q: InvocationAttemptStarted %v\n", req.ProjectId, event.InvocationAttemptStarted.AttemptNumber)
	case *buildeventpb.BuildEvent_InvocationAttemptFinished_:
		fmt.Printf("PublishLifecycleEvent %q: InvocationAttemptFinished %v\n", req.ProjectId, event.InvocationAttemptFinished.InvocationStatus)
	case *buildeventpb.BuildEvent_BuildFinished_:
		fmt.Printf("PublishLifecycleEvent %q: BuildFinished %v\n", req.ProjectId, event.BuildFinished.Details)
	default:
		fmt.Printf("PublishLifecycleEvent %q: unknown %T %v\n", req.ProjectId, event, event)
	}

	return &emptypb.Empty{}, nil
}
