package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/sirupsen/logrus"
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
	key := storage.BlobKey{
		InstanceName: req.InstanceName,
		Kind:         storage.CONTENT_ACTION,
		Digest:       req.ActionDigest.Hash,
		Size:         int(req.ActionDigest.SizeBytes),
		ExpiresMin:   12 * time.Hour,
	}

	var ar execpb.ActionResult
	err := c.DownloadProto(ctx, key, &ar)
	if err != nil {
		if status.Code(err) == codes.Unknown {
			logrus.Errorf("GetActionResult Err %q (code %v) %v", key, status.Code(err), err)
			if strings.Contains(err.Error(), "unmarshal") {
				// Could not unmarshal probably means a corrupt protobuf uploaded
				logrus.Errorf("GetActionResult deleting probably corrupt object")
				// go c.DeleteObject(&s3.DeleteObjectInput{
				// 	Bucket: &c.bucket,
				// 	Key:    aws.String(key.S3Key()),
				// })
			}
		}

		return nil, status.Error(codes.NotFound, err.Error())
	}

	// check CAS to make sure blobs are still available
	blobs := make([]*execpb.Digest, 0, 1+len(ar.OutputFiles)+len(ar.OutputDirectories))
	blobs = append(blobs, req.ActionDigest)
	for _, file := range ar.OutputFiles {
		blobs = append(blobs, file.Digest)
	}
	for _, dir := range ar.OutputDirectories {
		blobs = append(blobs, dir.TreeDigest)
	}
	missing, err := c.FindMissingBlobs(ctx, &execpb.FindMissingBlobsRequest{
		InstanceName: req.InstanceName,
		BlobDigests:  blobs,
	})
	if err != nil || len(missing.MissingBlobDigests) > 0 {
		// couldn't find some blobs, return NotFound so bazel will re-run and re-upload
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Missing blobs (%v/%v): %v", len(missing.MissingBlobDigests), len(blobs), err))
	}

	logrus.Infof("Action result cached: %#v", ar)

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
//   - `INVALID_ARGUMENT`: One or more arguments are invalid.
//   - `FAILED_PRECONDITION`: One or more errors occurred in updating the
//     action result, such as a missing command or action.
//   - `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
//     entry to the cache.
func (c *Server) UpdateActionResult(ctx context.Context, req *execpb.UpdateActionResultRequest) (*execpb.ActionResult, error) {
	key := storage.BlobKey{
		InstanceName: req.InstanceName,
		Kind:         storage.CONTENT_ACTION,
		Digest:       req.ActionDigest.Hash,
		Size:         int(req.ActionDigest.SizeBytes),
		ExpiresMin:   12 * time.Hour,
	}

	// logrus.Infof("UpdateActionResult %q\n", key)

	err := c.UploadProto(ctx, key, req.ActionResult)

	return req.ActionResult, err
}
