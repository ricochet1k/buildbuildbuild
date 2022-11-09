package server

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
)

// Determine if blobs are present in the CAS.
//
// Clients can use this API before uploading blobs to determine which ones are
// already present in the CAS and do not need to be uploaded again.
//
// Servers SHOULD increase the lifetimes of the referenced blobs if necessary and
// applicable.
//
// There are no method-specific errors.

func (c *Server) FindMissingBlobs(ctx context.Context, req *execpb.FindMissingBlobsRequest) (*execpb.FindMissingBlobsResponse, error) {
	eg, ctx := errgroup.WithContext(ctx)

	allMissing := make(chan []*execpb.Digest, 1)
	missing := make(chan *execpb.Digest)
	go func() {
		missingDigests := []*execpb.Digest{}
		for digest := range missing {
			missingDigests = append(missingDigests, digest)
		}
		allMissing <- missingDigests
		close(allMissing)
	}()

	for _, digest := range req.BlobDigests {
		digest := digest // no closure capture bugs
		key := StorageKey(req.InstanceName, CONTENT_CAS, DigestKey(digest))
		eg.Go(func() error {
			_, err := c.downloader.S3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: &c.bucket,
				Key:    &key,
			})
			if err != nil {
				missing <- digest
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		fmt.Printf("FindMissingBlobs Err %v\n", err)
		return nil, err
	}

	close(missing)
	missingDigests := <-allMissing

	return &execpb.FindMissingBlobsResponse{
		MissingBlobDigests: missingDigests,
	}, nil
}

// Upload many blobs at once.
//
// The server may enforce a limit of the combined total size of blobs
// to be uploaded using this API. This limit may be obtained using the
// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
// Requests exceeding the limit should either be split into smaller
// chunks or uploaded using the
// [ByteStream API][google.bytestream.ByteStream], as appropriate.
//
// This request is equivalent to calling a Bytestream `Write` request
// on each individual blob, in parallel. The requests may succeed or fail
// independently.
//
// Errors:
//
// * `INVALID_ARGUMENT`: The client attempted to upload more than the
//   server supported limit.
//
// Individual requests may return the following errors, additionally:
//
// * `RESOURCE_EXHAUSTED`: There is insufficient disk quota to store the blob.
// * `INVALID_ARGUMENT`: The
// [Digest][build.bazel.remote.execution.v2.Digest] does not match the
// provided data.
func (c *Server) BatchUpdateBlobs(ctx context.Context, reqs *execpb.BatchUpdateBlobsRequest) (*execpb.BatchUpdateBlobsResponse, error) {
	// default to expiring in a day. Push API or other APIs will kick this forward later if necessary
	expires := time.Now().Add(24 * time.Hour)
	metadata := map[string]*string{
		METADATA_EXPIRES: proto.String(expires.String()),
	}

	responses := make(chan *execpb.BatchUpdateBlobsResponse_Response)
	for _, req := range reqs.Requests {
		go func(req *execpb.BatchUpdateBlobsRequest_Request) {
			key := StorageKey(reqs.InstanceName, CONTENT_CAS, DigestKey(req.Digest))
			_, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
				Body:     bytes.NewReader(req.Data),
				Bucket:   &c.bucket,
				Key:      &key,
				Expires:  &time.Time{},
				Metadata: metadata,
			})
			if err != nil {
				fmt.Printf("Upload Error: %v\n", err)
			}

			s, _ := status.FromError(err)

			responses <- &execpb.BatchUpdateBlobsResponse_Response{
				Digest: req.Digest,
				Status: s.Proto(),
			}
		}(req)
	}
	resps := make([]*execpb.BatchUpdateBlobsResponse_Response, len(reqs.Requests))
	for i := range reqs.Requests {
		resps[i] = <-responses
	}
	return &execpb.BatchUpdateBlobsResponse{
		Responses: resps,
	}, nil
}

// Download many blobs at once.
//
// The server may enforce a limit of the combined total size of blobs
// to be downloaded using this API. This limit may be obtained using the
// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
// Requests exceeding the limit should either be split into smaller
// chunks or downloaded using the
// [ByteStream API][google.bytestream.ByteStream], as appropriate.
//
// This request is equivalent to calling a Bytestream `Read` request
// on each individual blob, in parallel. The requests may succeed or fail
// independently.
//
// Errors:
//
// * `INVALID_ARGUMENT`: The client attempted to read more than the
//   server supported limit.
//
// Every error on individual read will be returned in the corresponding digest
// status.

func (c *Server) BatchReadBlobs(ctx context.Context, reqs *execpb.BatchReadBlobsRequest) (*execpb.BatchReadBlobsResponse, error) {
	responses := make(chan *execpb.BatchReadBlobsResponse_Response)
	for _, digest := range reqs.Digests {
		go func(digest *execpb.Digest) {
			var b aws.WriteAtBuffer
			key := StorageKey(reqs.InstanceName, CONTENT_CAS, DigestKey(digest))
			_, err := c.downloader.DownloadWithContext(ctx, &b, &s3.GetObjectInput{
				Bucket: &c.bucket,
				Key:    &key,
			})

			s, _ := status.FromError(err)

			responses <- &execpb.BatchReadBlobsResponse_Response{
				Digest: digest,
				Data:   b.Bytes(),
				// Compressor: 0,
				Status: s.Proto(),
			}
		}(digest)
	}
	resps := make([]*execpb.BatchReadBlobsResponse_Response, len(reqs.Digests))
	for i := range reqs.Digests {
		resps[i] = <-responses
	}
	return &execpb.BatchReadBlobsResponse{
		Responses: resps,
	}, nil
}

// Fetch the entire directory tree rooted at a node.
//
// This request must be targeted at a
// [Directory][build.bazel.remote.execution.v2.Directory] stored in the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
// (CAS). The server will enumerate the `Directory` tree recursively and
// return every node descended from the root.
//
// The GetTreeRequest.page_token parameter can be used to skip ahead in
// the stream (e.g. when retrying a partially completed and aborted request),
// by setting it to a value taken from GetTreeResponse.next_page_token of the
// last successfully processed GetTreeResponse).
//
// The exact traversal order is unspecified and, unless retrieving subsequent
// pages from an earlier request, is not guaranteed to be stable across
// multiple invocations of `GetTree`.
//
// If part of the tree is missing from the CAS, the server will return the
// portion present and omit the rest.
//
// Errors:
//
// * `NOT_FOUND`: The requested tree root is not present in the CAS.

func (c *Server) GetTree(req *execpb.GetTreeRequest, gts execpb.ContentAddressableStorage_GetTreeServer) error {
	fmt.Println("Unimplemented: GetTree")
	return status.Error(codes.Unimplemented, "GetTree not implemented")
}
