package server

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/protobuf/proto"

	assetpb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func QualifiersToString(qualifiers []*assetpb.Qualifier) string {
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
	return qualstr
}

func (c *Server) EnsureExpiresAfter(ctx context.Context, instanceName string, expires time.Time, digests []*execpb.Digest) error {
	expiresb, err := expires.MarshalText()
	if err != nil {
		return err
	}
	expiresstr := string(expiresb)

	eg, ctx := errgroup.WithContext(ctx)
	for _, digest := range digests {
		digest := digest // avoid closure capture of loop variable that changes
		eg.Go(func() error {
			key := StorageKey(instanceName, CONTENT_ASSET, DigestKey(digest))
			out, err := c.downloader.S3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: &c.bucket,
				Key:    &key,
			})
			if err != nil {
				fmt.Printf("EnsureExpiresAfter Err %q %v\n", key, err)
				return err
			}

			refmetadata := out.Metadata
			refexpiresptr := refmetadata[METADATA_EXPIRES]
			refexpires := ""
			if refexpiresptr != nil {
				refexpires = *refexpiresptr
			}

			if refexpires < expiresstr {
				refmetadata[METADATA_EXPIRES] = &expiresstr
				_, err := c.uploader.S3.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
					Bucket:     &c.bucket,
					CopySource: new(string),
					Expires:    &expires,
					Key:        &key,
					Metadata:   refmetadata,
				})
				if err != nil {
					fmt.Printf("EnsureExpiresAfter Err %q %v\n", key, err)
					return err
				}
			}

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *Server) UploadAll(ctx context.Context, instanceName string, expires time.Time, uris []string, keysuffix string, metadata map[string]*string, body []byte) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, uri := range uris {
		uri := uri // avoid closure capture of loop variable that changes
		eg.Go(func() error {
			key := StorageKey(instanceName, CONTENT_ASSET, uri+keysuffix)
			_, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
				Body:     bytes.NewReader(body),
				Bucket:   &c.bucket,
				Key:      &key,
				Expires:  &expires, // this is when cache expires, not when to delete it
				Metadata: metadata,
			})
			return err
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
// * `INVALID_ARGUMENT`: One or more arguments to the RPC were invalid.
// * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to
//   perform the requested operation. The client may retry after a delay.
// * `UNAVAILABLE`: Due to a transient condition the operation could not be
//   completed. The client should retry.
// * `INTERNAL`: An internal error occurred while performing the operation.
//   The client should retry.
func (c *Server) PushBlob(ctx context.Context, req *assetpb.PushBlobRequest) (*assetpb.PushBlobResponse, error) {
	expires := req.ExpireAt.AsTime()
	body, _ := proto.Marshal(req)

	qualstr := QualifiersToString(req.Qualifiers)

	metadata := map[string]*string{
		METADATA_EXPIRES: proto.String(expires.String()),
	}
	for _, qual := range req.Qualifiers {
		metadata["Q-"+qual.Name] = proto.String(qual.Value)
	}

	// Update the expires metadata on all the blobs
	digests := []*execpb.Digest{req.BlobDigest}
	digests = append(digests, req.ReferencesBlobs...)
	digests = append(digests, req.ReferencesDirectories...)
	if err := c.EnsureExpiresAfter(ctx, req.InstanceName, expires, digests); err != nil {
		fmt.Printf("PushBlob Err %v\n", err)
		return nil, err
	}

	if err := c.UploadAll(ctx, req.InstanceName, expires, req.Uris, qualstr, metadata, body); err != nil {
		fmt.Printf("PushBlob Err %v\n", err)
		return nil, err
	}

	return &assetpb.PushBlobResponse{}, nil
}

// this is a near-exact copy of PushBlob :/
func (c *Server) PushDirectory(ctx context.Context, req *assetpb.PushDirectoryRequest) (*assetpb.PushDirectoryResponse, error) {
	expires := req.ExpireAt.AsTime()
	body, _ := proto.Marshal(req)

	qualstr := QualifiersToString(req.Qualifiers)

	metadata := map[string]*string{
		METADATA_EXPIRES: proto.String(expires.String()),
	}
	for _, qual := range req.Qualifiers {
		metadata["Q-"+qual.Name] = proto.String(qual.Value)
	}

	// Update the expires metadata on all the blobs
	digests := []*execpb.Digest{req.RootDirectoryDigest}
	digests = append(digests, req.ReferencesBlobs...)
	digests = append(digests, req.ReferencesDirectories...)
	if err := c.EnsureExpiresAfter(ctx, req.InstanceName, expires, digests); err != nil {
		fmt.Printf("PushDirectory Err %v\n", err)
		return nil, err
	}

	if err := c.UploadAll(ctx, req.InstanceName, expires, req.Uris, qualstr, metadata, body); err != nil {
		fmt.Printf("PushDirectory Err %v\n", err)
		return nil, err
	}

	return &assetpb.PushDirectoryResponse{}, nil
}
