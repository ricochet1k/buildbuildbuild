package server

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Retrieve a cached execution result.
//
// Implementations SHOULD ensure that any blobs referenced from the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
// are available at the time of returning the
// [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
// for some period of time afterwards. The lifetimes of the referenced blobs SHOULD be increased
// if necessary and applicable.
//
// Errors:
//
// * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
func (c *Server) GetActionResult(ctx context.Context, req *execpb.GetActionResultRequest) (*execpb.ActionResult, error) {
	key := StorageKey(req.InstanceName, CONTENT_ACTION, DigestKey(req.ActionDigest))

	buf := aws.NewWriteAtBuffer(make([]byte, 0, 10*1024))

	_, err := c.downloader.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		// fmt.Printf("ActionResult Failed to download %q: %v\n", key, err)
		return nil, status.Error(codes.NotFound, err.Error())
	}

	var ar execpb.ActionResult
	err = proto.Unmarshal(buf.Bytes(), &ar)
	if err != nil {
		fmt.Printf("GetActionResult Err %q %v\n", key, err)
		c.downloader.S3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: &c.bucket,
			Key:    aws.String(key),
		})
		return nil, err
	}

	// TODO: check CAS to make sure blobs are still available

	// fmt.Printf("ActionResult YAY! %q\n", key)

	return &ar, nil
}

// Upload a new execution result.
//
// In order to allow the server to perform access control based on the type of
// action, and to assist with client debugging, the client MUST first upload
// the [Action][build.bazel.remote.execution.v2.Execution] that produced the
// result, along with its
// [Command][build.bazel.remote.execution.v2.Command], into the
// `ContentAddressableStorage`.
//
// Server implementations MAY modify the
// `UpdateActionResultRequest.action_result` and return an equivalent value.
//
// Errors:
//
// * `INVALID_ARGUMENT`: One or more arguments are invalid.
// * `FAILED_PRECONDITION`: One or more errors occurred in updating the
//   action result, such as a missing command or action.
// * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
//   entry to the cache.
func (c *Server) UpdateActionResult(ctx context.Context, req *execpb.UpdateActionResultRequest) (*execpb.ActionResult, error) {
	key := StorageKey(req.InstanceName, CONTENT_ACTION, DigestKey(req.ActionDigest))

	// fmt.Printf("UpdateActionResult %q\n", key)

	body, err := proto.Marshal(req.ActionResult)
	if err != nil {
		fmt.Printf("Failed to marshal ActionResult: %v", err)
		return nil, err
	}

	_, err = c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: &c.bucket,
		Body:   bytes.NewReader(body),
		Key:    aws.String(key),
	})
	if err != nil {
		fmt.Printf("Failed to upload: %v", err)
		return nil, err
	}

	return req.ActionResult, nil
}
