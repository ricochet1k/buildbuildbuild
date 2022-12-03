package server

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/sirupsen/logrus"
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

	// logrus.Printf("FindMissingBlobs: %v", req)

	missing := make(chan *execpb.Digest)
	go func() {
		for _, digest := range req.BlobDigests {
			if digest == nil {
				continue
			}
			digest := digest // no closure capture bugs
			key := storage.BlobKey{InstanceName: req.InstanceName, Kind: storage.CONTENT_CAS, Digest: digest.Hash, Size: int(digest.SizeBytes), ExpiresMin: 12 * time.Hour}
			eg.Go(func() error {
				if _, err := c.Exists(ctx, key); err != nil {
					missing <- digest
				}
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			logrus.Printf("FindMissingBlobs Err %v\n", err)
		}

		close(missing)
	}()

	missingDigests := []*execpb.Digest{}
	for digest := range missing {
		missingDigests = append(missingDigests, digest)
	}

	// if len(missingDigests) > 0 {
	// 	logrus.Printf("FindMissingBlobs missing: %v", missingDigests)
	// }

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
//   - `INVALID_ARGUMENT`: The client attempted to upload more than the
//     server supported limit.
//
// Individual requests may return the following errors, additionally:
//
// * `RESOURCE_EXHAUSTED`: There is insufficient disk quota to store the blob.
// * `INVALID_ARGUMENT`: The
// [Digest][build.bazel.remote.execution.v2.Digest] does not match the
// provided data.
func (c *Server) BatchUpdateBlobs(ctx context.Context, reqs *execpb.BatchUpdateBlobsRequest) (*execpb.BatchUpdateBlobsResponse, error) {
	var fakekey storage.BlobKey
	fakekey.SetMetadataFromContext(ctx)

	responses := make(chan *execpb.BatchUpdateBlobsResponse_Response)
	for _, req := range reqs.Requests {
		req := req
		go func() {
			key := storage.BlobKey{
				InstanceName: reqs.InstanceName,
				Kind:         storage.CONTENT_CAS,
				Digest:       req.Digest.Hash,
				Size:         int(req.Digest.SizeBytes),
				Compressor:   req.Compressor,
				Metadata:     fakekey.Metadata,
				ExpiresMin:   24 * time.Hour,
			}
			err := c.UploadBytes(ctx, key, req.Data)
			if err != nil {
				logrus.Printf("Upload Error: %v\n", err)
			}

			s, _ := status.FromError(err)

			responses <- &execpb.BatchUpdateBlobsResponse_Response{
				Digest: req.Digest,
				Status: s.Proto(),
			}
		}()
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
		digest := digest
		go func() {
			key := storage.BlobKey{
				InstanceName: reqs.InstanceName,
				Kind:         storage.CONTENT_CAS,
				Digest:       digest.Hash,
				Size:         int(digest.SizeBytes),
				Metadata:     map[string]*string{},
			}
			buf, _, err := c.DownloadBytes(ctx, key)

			s, _ := status.FromError(err)

			responses <- &execpb.BatchReadBlobsResponse_Response{
				Digest:     digest,
				Data:       buf.Bytes(),
				Compressor: execpb.Compressor_IDENTITY,
				Status:     s.Proto(),
			}
		}()
	}
	resps := make([]*execpb.BatchReadBlobsResponse_Response, 0, len(reqs.Digests))
	for range reqs.Digests {
		resps = append(resps, <-responses)
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
	logrus.Errorf("Unimplemented: GetTree")
	return status.Error(codes.Unimplemented, "GetTree not implemented")
}
