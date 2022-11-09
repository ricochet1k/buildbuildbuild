package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	assetpb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type AssetType interface {
	MainDigest() *execpb.Digest
	Proto() proto.Message
	GetReferencesBlobs() []*execpb.Digest
	GetReferencesDirectories() []*execpb.Digest
}

type BlobAsset struct {
	*assetpb.PushBlobRequest
}

func (a *BlobAsset) MainDigest() *execpb.Digest {
	return a.BlobDigest
}
func (a *BlobAsset) Proto() proto.Message {
	return a.PushBlobRequest
}

type DirectoryAsset struct {
	*assetpb.PushDirectoryRequest
}

func (a *DirectoryAsset) MainDigest() *execpb.Digest {
	return a.RootDirectoryDigest
}
func (a *DirectoryAsset) Proto() proto.Message {
	return a.PushDirectoryRequest
}

func (c *Server) HeadAndDownload(ctx context.Context, key string) (*s3.HeadObjectOutput, []byte, error) {
	eg, ctx := errgroup.WithContext(ctx)
	var head *s3.HeadObjectOutput
	var body []byte

	eg.Go(func() error {
		head_, err := c.downloader.S3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: &c.bucket,
			Key:    &key,
		})
		head = head_
		return err
	})

	eg.Go(func() error {
		var b aws.WriteAtBuffer
		_, err := c.downloader.DownloadWithContext(ctx, &b, &s3.GetObjectInput{
			Bucket: &c.bucket,
			Key:    &key,
		})
		if err != nil {
			return err
		}
		body = b.Bytes()
		return nil
	})

	err := eg.Wait()
	return head, body, err
}

func (c *Server) FetchAsset(ctx context.Context, instanceName string, uris []string, qualifiers []*assetpb.Qualifier, asset AssetType) (string, []*assetpb.Qualifier, time.Time, error) {
	qualstr := QualifiersToString(qualifiers)
	var uri, key string
	var head *s3.HeadObjectOutput
	var body []byte
	var err error

	for _, uri_ := range uris {
		uri = uri_
		key = StorageKey(instanceName, CONTENT_ASSET, uri+qualstr)

		head, body, err = c.HeadAndDownload(ctx, key)
		if err != nil {
			fmt.Printf("FetchAsset key %q get: %v", key, err)
			continue
		}
		break
	}
	if err != nil {
		fmt.Printf("FetchAsset failed")
		return "", nil, time.Now(), status.Error(codes.NotFound, "No URIs found")
	}

	expiresptr := head.Metadata[METADATA_EXPIRES]
	expiresstr := ""
	if expiresptr != nil {
		expiresstr = *expiresptr
	}
	expires, err := time.Parse(time.RFC3339, expiresstr)
	if err != nil {
		fmt.Printf("FetchAsset could not parse expires %q: %v", expiresstr, err)
		expires = time.Now()
	}

	tomorrow := time.Now().Add(24 * time.Hour)
	if expires.Before(tomorrow) {
		expires = tomorrow
	}

	if err := proto.Unmarshal(body, asset.Proto()); err != nil {
		fmt.Printf("FetchAsset key %q: Error unmarshaling as %T: %v", key, asset.Proto(), err)
		return "", nil, time.Now(), status.Error(codes.NotFound, "Invalid format")
	}

	// Update the expires metadata on all the blobs
	// Also makes sure they all still exist
	digests := []*execpb.Digest{asset.MainDigest()}
	digests = append(digests, asset.GetReferencesBlobs()...)
	digests = append(digests, asset.GetReferencesDirectories()...)
	if err := c.EnsureExpiresAfter(ctx, instanceName, expires, digests); err != nil {
		return "", nil, time.Now(), err
	}

	qualifiers = []*assetpb.Qualifier{}
	for k, v := range head.Metadata {
		if strings.HasPrefix(k, "Q-") {
			qualifiers = append(qualifiers, &assetpb.Qualifier{
				Name:  k[2:],
				Value: *v,
			})
		}
	}

	return uri, qualifiers, expires, nil
}

// Resolve or fetch referenced assets, making them available to the caller and
// other consumers in the [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
//
// Servers *MAY* fetch content that they do not already have cached, for any
// URLs they support.
//
// Servers *SHOULD* ensure that referenced files are present in the CAS at the
// time of the response, and (if supported) that they will remain available
// for a reasonable period of time. The lifetimes of the referenced blobs *SHOULD*
// be increased if necessary and applicable.
// In the event that a client receives a reference to content that is no
// longer present, it *MAY* re-issue the request with
// `oldest_content_accepted` set to a more recent timestamp than the original
// attempt, to induce a re-fetch from origin.
//
// Servers *MAY* cache fetched content and reuse it for subsequent requests,
// subject to `oldest_content_accepted`.
//
// Servers *MAY* support the complementary [Push][build.bazel.remote.asset.v1.Push]
// API and allow content to be directly inserted for use in future fetch
// responses.
//
// Servers *MUST* ensure Fetch'd content matches all the specified
// qualifiers except in the case of previously Push'd resources, for which
// the server *MAY* trust the pushing client to have set the qualifiers
// correctly, without validation.
//
// Servers not implementing the complementary [Push][build.bazel.remote.asset.v1.Push]
// API *MUST* reject requests containing qualifiers it does not support.
//
// Servers *MAY* transform assets as part of the fetch. For example a
// tarball fetched by [FetchDirectory][build.bazel.remote.asset.v1.Fetch.FetchDirectory]
// might be unpacked, or a Git repository
// fetched by [FetchBlob][build.bazel.remote.asset.v1.Fetch.FetchBlob]
// might be passed through `git-archive`.
//
// Errors handling the requested assets will be returned as gRPC Status errors
// here; errors outside the server's control will be returned inline in the
// `status` field of the response (see comment there for details).
// The possible RPC errors include:
// * `INVALID_ARGUMENT`: One or more arguments were invalid, such as a
//   qualifier that is not supported by the server.
// * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to
//   perform the requested operation. The client may retry after a delay.
// * `UNAVAILABLE`: Due to a transient condition the operation could not be
//   completed. The client should retry.
// * `INTERNAL`: An internal error occurred while performing the operation.
//   The client should retry.
// * `DEADLINE_EXCEEDED`: The fetch could not be completed within the given
//   RPC deadline. The client should retry for at least as long as the value
//   provided in `timeout` field of the request.
//
// In the case of unsupported qualifiers, the server *SHOULD* additionally
// send a [BadRequest][google.rpc.BadRequest] error detail where, for each
// unsupported qualifier, there is a `FieldViolation` with a `field` of
// `qualifiers.name` and a `description` of `"{qualifier}" not supported`
// indicating the name of the unsupported qualifier.
func (c *Server) FetchBlob(ctx context.Context, req *assetpb.FetchBlobRequest) (*assetpb.FetchBlobResponse, error) {
	var asset BlobAsset

	uri, qualifiers, expires, err := c.FetchAsset(ctx, req.InstanceName, req.Uris, req.Qualifiers, &asset)
	if err != nil {
		fmt.Printf("FetchBlob Err %v\n", err)
		return nil, err
	}

	return &assetpb.FetchBlobResponse{
		Status:     status.Convert(nil).Proto(),
		Uri:        uri,
		Qualifiers: qualifiers,
		ExpiresAt:  timestamppb.New(expires),
		BlobDigest: asset.BlobDigest,
	}, nil
}

func (c *Server) FetchDirectory(ctx context.Context, req *assetpb.FetchDirectoryRequest) (*assetpb.FetchDirectoryResponse, error) {
	var asset DirectoryAsset

	uri, qualifiers, expires, err := c.FetchAsset(ctx, req.InstanceName, req.Uris, req.Qualifiers, &asset)
	if err != nil {
		fmt.Printf("FetchDirectory Err %v\n", err)
		return nil, err
	}

	return &assetpb.FetchDirectoryResponse{
		Status:              status.Convert(nil).Proto(),
		Uri:                 uri,
		Qualifiers:          qualifiers,
		ExpiresAt:           timestamppb.New(expires),
		RootDirectoryDigest: asset.RootDirectoryDigest,
	}, nil
}
