package server

import (
	"context"
	"fmt"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/golang/protobuf/proto"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/sirupsen/logrus"

	assetpb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func QualifiersToString(qualifiers []*assetpb.Qualifier) string {
	if len(qualifiers) == 0 {
		return ""
	}

	qualMap := map[string]string{}
	quals := []string{}
	for _, qual := range qualifiers {
		quals = append(quals, qual.Name)
		qualMap[qual.Name] = qual.Value
	}
	sort.Strings(quals)

	qualstr := ""
	for _, qual := range quals {
		qualstr += "," + qual + "=" + qualMap[qual]
	}
	return " Q " + qualstr
}

func (c *Server) EnsureExpiresAfter(ctx context.Context, instanceName string, expiresMin time.Duration, digests []*execpb.Digest) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, digest := range digests {
		digest := digest // avoid closure capture of loop variable that changes
		eg.Go(func() error {
			key := storage.BlobKey{
				InstanceName: instanceName,
				Kind:         storage.CONTENT_CAS,
				Digest:       digest.Hash,
				Size:         int(digest.SizeBytes),
				ExpiresMin:   expiresMin,
			}
			_, err := c.Exists(ctx, key)
			if err != nil {
				fmt.Printf("EnsureExpiresAfter Err %v %v\n", key, err)
				return err
			}

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *Server) UploadAll(ctx context.Context, instanceName string, expiresMin time.Duration, uris []string, keysuffix string, metadata storage.Metadata, body []byte) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, uri := range uris {
		uri := uri // avoid closure capture of loop variable that changes
		eg.Go(func() error {
			// TODO: expires
			key := storage.BlobKey{InstanceName: instanceName, Kind: storage.CONTENT_ASSET, Digest: uri + keysuffix, Size: len(body), Metadata: metadata, ExpiresMin: expiresMin}
			return c.UploadBytes(ctx, key, body)
		})
	}
	if err := eg.Wait(); err != nil {
		fmt.Printf("UploadAll Err %v\n", err)
		return err
	}
	return nil
}

// These APIs associate the identifying information of a resource, as
// indicated by URI and optionally Qualifiers, with content available in the
// CAS. For example, associating a repository url and a commit id with a
// Directory Digest.
//
// Servers *SHOULD* only allow trusted clients to associate content, and *MAY*
// only allow certain URIs to be pushed.
//
// Clients *MUST* ensure associated content is available in CAS prior to
// pushing.
//
// Clients *MUST* ensure the Qualifiers listed correctly match the contents,
// and Servers *MAY* trust these values without validation.
// Fetch servers *MAY* require exact match of all qualifiers when returning
// content previously pushed, or allow fetching content with only a subset of
// the qualifiers specified on Push.
//
// Clients can specify expiration information that the server *SHOULD*
// respect. Subsequent requests can be used to alter the expiration time.
//
// A minimal compliant Fetch implementation may support only Push'd content
// and return `NOT_FOUND` for any resource that was not pushed first.
// Alternatively, a compliant implementation may choose to not support Push
// and only return resources that can be Fetch'd from origin.
//
// Errors will be returned as gRPC Status errors.
// The possible RPC errors include:
//   - `INVALID_ARGUMENT`: One or more arguments to the RPC were invalid.
//   - `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to
//     perform the requested operation. The client may retry after a delay.
//   - `UNAVAILABLE`: Due to a transient condition the operation could not be
//     completed. The client should retry.
//   - `INTERNAL`: An internal error occurred while performing the operation.
//     The client should retry.
func (c *Server) PushBlob(ctx context.Context, req *assetpb.PushBlobRequest) (*assetpb.PushBlobResponse, error) {
	expiresMin := req.ExpireAt.AsTime().Sub(time.Now())
	body, _ := proto.Marshal(req)

	qualstr := QualifiersToString(req.Qualifiers)

	logrus.Infof("PushBlob: %v %v %v", len(req.Uris), req.Uris[0], qualstr)

	metadata := storage.Metadata{}
	for _, qual := range req.Qualifiers {
		metadata.Put("Q-"+qual.Name, qual.Value)
	}

	// Update the expires metadata on all the blobs
	digests := []*execpb.Digest{req.BlobDigest}
	digests = append(digests, req.ReferencesBlobs...)
	digests = append(digests, req.ReferencesDirectories...)
	if err := c.EnsureExpiresAfter(ctx, req.InstanceName, expiresMin, digests); err != nil {
		fmt.Printf("PushBlob Err %v\n", err)
		return nil, err
	}

	if err := c.UploadAll(ctx, req.InstanceName, expiresMin, req.Uris, qualstr, metadata, body); err != nil {
		fmt.Printf("PushBlob Err %v\n", err)
		return nil, err
	}

	return &assetpb.PushBlobResponse{}, nil
}

// this is a near-exact copy of PushBlob :/
func (c *Server) PushDirectory(ctx context.Context, req *assetpb.PushDirectoryRequest) (*assetpb.PushDirectoryResponse, error) {
	expiresMin := req.ExpireAt.AsTime().Sub(time.Now())
	body, _ := proto.Marshal(req)

	qualstr := QualifiersToString(req.Qualifiers)

	logrus.Infof("PushDirectory: %v %v %v", len(req.Uris), req.Uris[0], qualstr)

	metadata := storage.Metadata{}
	for _, qual := range req.Qualifiers {
		metadata.Put("Q-"+qual.Name, qual.Value)
	}

	// Update the expires metadata on all the blobs
	digests := []*execpb.Digest{req.RootDirectoryDigest}
	digests = append(digests, req.ReferencesBlobs...)
	digests = append(digests, req.ReferencesDirectories...)
	if err := c.EnsureExpiresAfter(ctx, req.InstanceName, expiresMin, digests); err != nil {
		fmt.Printf("PushDirectory Err %v\n", err)
		return nil, err
	}

	if err := c.UploadAll(ctx, req.InstanceName, expiresMin, req.Uris, qualstr, metadata, body); err != nil {
		fmt.Printf("PushDirectory Err %v\n", err)
		return nil, err
	}

	return &assetpb.PushDirectoryResponse{}, nil
}
