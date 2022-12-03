package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
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
	assetpb.PushBlobRequest
}

func (a *BlobAsset) MainDigest() *execpb.Digest {
	return a.BlobDigest
}
func (a *BlobAsset) Proto() proto.Message {
	return &a.PushBlobRequest
}

type DirectoryAsset struct {
	assetpb.PushDirectoryRequest
}

func (a *DirectoryAsset) MainDigest() *execpb.Digest {
	return a.RootDirectoryDigest
}
func (a *DirectoryAsset) Proto() proto.Message {
	return &a.PushDirectoryRequest
}

const ASSET_EXPIRE_AFTER = 7 * 24 * time.Hour

func (c *Server) TryDownload(ctx context.Context, uris []string) (*os.File, string, string, int64, error) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(fmt.Sprintf("Could not create temp file: %v", err))
	}

	for _, uri := range uris {
		logrus.Infof("FetchAsset downloading asset: %q", uri)

		err := f.Truncate(0)
		if err != nil {
			panic(fmt.Sprintf("Could not truncate temp file: %v", err))
		}
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			panic(fmt.Sprintf("Could not seek temp file: %v", err))
		}

		resp, err := http.DefaultClient.Get(uri)
		if err != nil {
			logrus.Errorf("Failed to get %q: %v", uri, err)
			continue
		}

		h := sha256.New()

		w := io.MultiWriter(f, h)

		if _, err := io.Copy(w, resp.Body); err != nil {
			logrus.Errorf("Failed to download %q: %v", uri, err)
			continue
		}

		rawhash := h.Sum(nil)
		hash := fmt.Sprintf("%x", rawhash)

		size, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			panic(fmt.Sprintf("Could not seek temp file: %v", err))
		}

		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			panic(fmt.Sprintf("Could not seek temp file: %v", err))
		}

		return f, uri, hash, size, nil
	}

	os.Remove(f.Name())

	return nil, "", "", 0, os.ErrNotExist
}

func (c *Server) DownloadUploadAsset(ctx context.Context, instanceName string, uris []string) (string, *execpb.Digest, error) {
	f, uri, hash, size, err := c.TryDownload(ctx, uris)
	if err != nil {
		return "", nil, err
	}

	defer os.Remove(f.Name())

	key := storage.BlobKey{
		InstanceName: instanceName,
		Kind:         storage.CONTENT_CAS,
		Digest:       hash,
		Size:         int(size),
		ExpiresMin:   ASSET_EXPIRE_AFTER,
	}
	key.Metadata.Put("Asset", uri)
	w, err := c.UploadWriter(ctx, key)
	if err != nil {
		return "", nil, err
	}
	logrus.Infof("FetchAsset uploading asset: %q %v %v", uri, hash, size)
	n, err := io.Copy(w, f)
	logrus.Infof("Uploaded %v bytes to %v: %v", n, hash, err)
	if err == nil {
		err = w.Finish()
	}
	if err != nil {
		return "", nil, err
	}
	err = w.Close()
	if err != nil {
		return "", nil, err
	}

	return uri, &execpb.Digest{
		Hash:      hash,
		SizeBytes: size,
	}, nil
}

func (c *Server) FetchAsset(ctx context.Context, instanceName string, uris []string, qualifiers []*assetpb.Qualifier, asset AssetType) (string, []*assetpb.Qualifier, error) {
	qualstr := QualifiersToString(qualifiers)

	logrus.Infof("FetchAsset: %v %q", uris, qualstr)

	type metadataBuffer struct {
		metadata map[string]*string
		body     *bytes.Buffer
	}

	funcs := []func(context.Context) (metadataBuffer, error){}
	for _, uri := range uris {
		uri := uri
		key := storage.BlobKey{InstanceName: instanceName, Kind: storage.CONTENT_ASSET, Key: uri + qualstr, Size: -1, ExpiresMin: ASSET_EXPIRE_AFTER}

		funcs = append(funcs, func(ctx context.Context) (metadataBuffer, error) {
			body, metadata, err := c.DownloadBytes(ctx, key)
			return metadataBuffer{metadata, body}, err
		})
	}
	var uri string
	i, mb, err := utils.Race(ctx, funcs...)
	if err == nil {
		uri = uris[i]
		if err := proto.Unmarshal(mb.body.Bytes(), asset.Proto()); err != nil {
			fmt.Printf("FetchAsset uri %q: Error unmarshaling as %T: %v", uris[i], asset.Proto(), err)
			return "", nil, status.Error(codes.NotFound, "Invalid format")
		}

		// make sure the asset actually exists
		_, err = c.Exists(ctx, storage.BlobKey{
			InstanceName: instanceName,
			Kind:         storage.CONTENT_CAS,
			Digest:       asset.MainDigest().Hash,
			Size:         int(asset.MainDigest().SizeBytes),
		})

		if err == nil {
			// Update the expires metadata on all the blobs
			// Also makes sure they all still exist
			digests := []*execpb.Digest{asset.MainDigest()}
			digests = append(digests, asset.GetReferencesBlobs()...)
			digests = append(digests, asset.GetReferencesDirectories()...)
			if err := c.EnsureExpiresAfter(ctx, instanceName, ASSET_EXPIRE_AFTER, digests); err != nil {
				return "", nil, err
			}
		}
	}
	if err != nil {
		blobAsset := asset.(*BlobAsset)

		var digest *execpb.Digest
		uri, digest, err = c.DownloadUploadAsset(ctx, instanceName, uris)
		if err != nil {
			return "", nil, status.Error(codes.NotFound, "Unable to download")
		}

		blobAsset.PushBlobRequest = assetpb.PushBlobRequest{
			InstanceName: instanceName,
			Uris:         uris,
			Qualifiers:   qualifiers,
			BlobDigest:   digest,
			ExpireAt:     timestamppb.New(time.Now().Add(ASSET_EXPIRE_AFTER)),
		}

		key := storage.BlobKey{InstanceName: instanceName, Kind: storage.CONTENT_ASSET, Key: uri + qualstr, Size: -1, ExpiresMin: ASSET_EXPIRE_AFTER}
		err := c.UploadProto(ctx, key, &blobAsset.PushBlobRequest)
		if err != nil {
			return "", nil, status.Error(codes.NotFound, "Unable to upload asset")
		}
	}

	qualifiers = []*assetpb.Qualifier{}
	for k, v := range mb.metadata {
		if strings.HasPrefix(k, "Q-") {
			qualifiers = append(qualifiers, &assetpb.Qualifier{
				Name:  k[2:],
				Value: *v,
			})
		}
	}

	logrus.Infof("Fetched asset %q at %q", uri, asset.Proto().String())

	return uri, qualifiers, nil
}

// Qualifiers can be any Key-Value pair of strings. However, there are a few standard qualifiers mentioned in the API at present (although they are still optional to support for the server):
// resource_type: a description of the type of resource. This is where the client may specify that the resource is a git repo for example. The API states that values should be an existing media type as defined by IANA.
// checksum.sri: a checksum to verify the fetched data against, as described above.
// directory: a relative path of a subdirectory of the resource. It allows the client to get the Digest of only the subdirectory it is interested in.
// vcs.branch: a version control branch to checkout before calculating and returning the Digest.
// vcs.commit: a version control commit to checkout before calculating and returning the Digest.

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
//   - `INVALID_ARGUMENT`: One or more arguments were invalid, such as a
//     qualifier that is not supported by the server.
//   - `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to
//     perform the requested operation. The client may retry after a delay.
//   - `UNAVAILABLE`: Due to a transient condition the operation could not be
//     completed. The client should retry.
//   - `INTERNAL`: An internal error occurred while performing the operation.
//     The client should retry.
//   - `DEADLINE_EXCEEDED`: The fetch could not be completed within the given
//     RPC deadline. The client should retry for at least as long as the value
//     provided in `timeout` field of the request.
//
// In the case of unsupported qualifiers, the server *SHOULD* additionally
// send a [BadRequest][google.rpc.BadRequest] error detail where, for each
// unsupported qualifier, there is a `FieldViolation` with a `field` of
// `qualifiers.name` and a `description` of `"{qualifier}" not supported`
// indicating the name of the unsupported qualifier.
func (c *Server) FetchBlob(ctx context.Context, req *assetpb.FetchBlobRequest) (*assetpb.FetchBlobResponse, error) {
	var asset BlobAsset

	uri, qualifiers, err := c.FetchAsset(ctx, req.InstanceName, req.Uris, req.Qualifiers, &asset)
	if err != nil {
		logrus.Errorf("FetchBlob %q %v: %v\n", req.Uris[0], req.Qualifiers, err)
		return nil, err
	}

	return &assetpb.FetchBlobResponse{
		Status:     status.Convert(nil).Proto(),
		Uri:        uri,
		Qualifiers: qualifiers,
		ExpiresAt:  asset.ExpireAt,
		BlobDigest: asset.BlobDigest,
	}, nil
}

func (c *Server) FetchDirectory(ctx context.Context, req *assetpb.FetchDirectoryRequest) (*assetpb.FetchDirectoryResponse, error) {
	var asset DirectoryAsset

	uri, qualifiers, err := c.FetchAsset(ctx, req.InstanceName, req.Uris, req.Qualifiers, &asset)
	if err != nil {
		logrus.Errorf("FetchDirectory %q %v: %v\n", req.Uris, req.Qualifiers, err)
		return nil, err
	}

	return &assetpb.FetchDirectoryResponse{
		Status:              status.Convert(nil).Proto(),
		Uri:                 uri,
		Qualifiers:          qualifiers,
		ExpiresAt:           asset.ExpireAt,
		RootDirectoryDigest: asset.RootDirectoryDigest,
	}, nil
}
