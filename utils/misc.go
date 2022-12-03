package utils

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type response[T any] struct {
	i   int
	val T
	err error
}

// Race runs all functions at the same time, returning the first nil-error response (and index) and cancelling the rest
func Race[T any](ctx context.Context, f ...func(ctx context.Context) (T, error)) (int, T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	responseChan := make(chan response[T], 1)
	for i, f := range f {
		i := i
		f := f
		go func() {
			val, err := f(ctx)
			responseChan <- response[T]{i, val, err}
		}()
	}

	var err error // TODO: collect and return all errors
	for range f {
		resp := <-responseChan
		if resp.err != nil {
			err = resp.err
		} else {
			return resp.i, resp.val, nil
		}
	}

	var zero T
	return -1, zero, err
}

func StringMapToStringPtrMap(metadata map[string]string) map[string]*string {
	mdata := make(map[string]*string, len(metadata))
	for k, v := range metadata {
		mdata[k] = aws.String(v)
	}
	return mdata
}

func ExtractIncomingRequestMetadata(ctx context.Context) (metadata.MD, *execpb.RequestMetadata) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return md, GetRequestMetadata(md)
	}
	return nil, nil
}

func GetRequestMetadata(md metadata.MD) *execpb.RequestMetadata {
	// logrus.Infof("Incoming Metadata: %q", md)
	if reqmdraw := md.Get("build.bazel.remote.execution.v2.requestmetadata-bin"); len(reqmdraw) > 0 {
		var reqmd execpb.RequestMetadata
		if err := proto.Unmarshal([]byte(reqmdraw[0]), &reqmd); err == nil {
			// logrus.Infof("Request Metadata: %v", prototext.Format(&reqmd))
			return &reqmd
		}
	}
	return nil
}
