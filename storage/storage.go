package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"google.golang.org/grpc/metadata"
)

type Storage interface {
	DownloadReader(ctx context.Context, key BlobKey, offset, limit int) (io.ReadCloser, Metadata, error)
	DownloadBytes(ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error)
	UploadWriter(ctx context.Context, key BlobKey) (utils.WriteCloserFinish, error)
	UploadBytes(ctx context.Context, key BlobKey, data []byte) error
	Exists(ctx context.Context, key BlobKey) (Metadata, error)
}

const (
	METADATA_EXPIRES string = "BBB-Expires"
)

type ContentKind string

const (
	CONTENT_LOG    ContentKind = "log"
	CONTENT_ACTION ContentKind = "action"
	CONTENT_CAS    ContentKind = "cas"
	CONTENT_ASSET  ContentKind = "asset"
)

type BlobKey struct {
	InstanceName string
	Kind         ContentKind
	Key          string
	Digest       string
	Size         int

	Compressor execpb.Compressor_Value

	// if this is set, it will ensure that matching keys are set in the blob metadata
	Metadata Metadata

	// if this is set, it will ensure that the value will last for at least this long
	// if it needs to update the expiration date, it will use 2x this ExpiresMin to avoid changing it frequently
	ExpiresMin time.Duration
}

func (key *BlobKey) RequestMetadata() metadata.MD {
	md := metadata.MD{}
	if key.ExpiresMin > 0 {
		md.Set("buildbuildbuild.expires_min", fmt.Sprint(int64(key.ExpiresMin)))
	}

	for k, v := range key.Metadata {
		md.Append("buildbuildbuild.metadata", fmt.Sprintf("%v=%v", k, v))
	}

	return md
}

func (key *BlobKey) SetMetadataFromContext(ctx context.Context) {
	key.SetMetadataFromRequest(utils.ExtractIncomingRequestMetadata(ctx))
}

func (key *BlobKey) SetMetadataFromRequest(md metadata.MD, reqmd *execpb.RequestMetadata) {
	if md != nil {
		if expiresMinraw := md.Get("buildbuildbuild.expires_min"); len(expiresMinraw) >= 1 {
			if expiresMin, err := strconv.Atoi(expiresMinraw[0]); err != nil {
				key.ExpiresMin = time.Duration(expiresMin)
			}
		}
		if metadataRaw := md.Get("buildbuildbuild.metadata"); len(metadataRaw) >= 1 {
			for _, val := range metadataRaw {
				if k, v, ok := strings.Cut(val, "="); ok {
					key.Metadata.Put(k, v)
				}
			}
		}
	}

	if reqmd != nil {
		if reqmd.TargetId != "" {
			key.Metadata.Put("TargetId", reqmd.TargetId)
		}
		if reqmd.ActionMnemonic != "" {
			key.Metadata.Put("ActionMnemonic", reqmd.ActionMnemonic)
		}
		if reqmd.ConfigurationId != "" {
			key.Metadata.Put("ConfigurationId", reqmd.ConfigurationId)
		}
	}
}

func (b *BlobKey) Filename() string {
	if b.Key == "" {
		if b.Digest != "" && b.Size >= 0 {
			if strings.Contains(b.Digest, ":") {
				panic("Bad digest")
			}
			return fmt.Sprintf("%v/%v:%v", b.Kind, b.Digest, b.Size)
		} else {
			panic(fmt.Sprintf("Empty key/digest/size: %#v", b))
		}
	} else {
		return fmt.Sprintf("%v/%v", b.Kind, url.PathEscape(b.Key))
	}
}

func (key *BlobKey) InstancePath() string {
	suffix := ""
	if key.Compressor == execpb.Compressor_ZSTD {
		suffix = ".zstd"
	}
	return fmt.Sprintf("%v/%v%v", key.InstanceName, key.Filename(), suffix)
}

func (b *BlobKey) blobs() string {
	if b.Compressor == execpb.Compressor_IDENTITY {
		return "blobs"
	} else {
		return "compressed-blobs/zstd"
	}
}

func (b *BlobKey) ByteStreamWriteKey() string {
	return fmt.Sprintf("%v/uploads/whatever/%v/localcacheonly/%v", b.InstanceName, b.blobs(), b.Filename())
}

func (b *BlobKey) ByteStreamReadKey() string {
	return fmt.Sprintf("%v/%v/localcacheonly/%v", b.InstanceName, b.blobs(), b.Filename())
}

type Metadata map[string]*string

func (m Metadata) Get(k string) string {
	v := m[k]
	if v != nil {
		return *v
	}
	return ""
}
func (m *Metadata) Put(k, v string) {
	if *m == nil {
		*m = map[string]*string{}
	}
	(*m)[k] = aws.String(v)
}
func (m Metadata) ToPlainMap() map[string]string {
	if m == nil {
		return nil
	}
	pm := make(map[string]string, len(m))
	for k, v := range m {
		pm[k] = *v
	}
	return pm
}

func (key *BlobKey) UpdateMetadataExpires(existingMetadata Metadata) {
	if key.Metadata == nil {
		key.Metadata = Metadata{}
	}
	if exp, ok := existingMetadata[METADATA_EXPIRES]; ok {
		key.Metadata[METADATA_EXPIRES] = exp
	}
	if key.ExpiresMin > 0 {
		expiresAfter := time.Now().Add(key.ExpiresMin)
		expiresAfterb, _ := expiresAfter.MarshalText()
		expiresAfterstr := string(expiresAfterb)

		existingExpires := ""
		if refexpiresptr, ok := existingMetadata[METADATA_EXPIRES]; ok && refexpiresptr != nil {
			existingExpires = *refexpiresptr
		}

		if existingExpires < expiresAfterstr {
			key.Metadata.Put(METADATA_EXPIRES, expiresAfterstr)
		}
	}
}

func (key *BlobKey) NeedsMetadataUpdate(existingMetadata Metadata) bool {
	needsUpdate := false
	key.UpdateMetadataExpires(existingMetadata)
	for k, v := range existingMetadata {
		if mv := key.Metadata[k]; mv == nil {
			key.Metadata[k] = v
		}
	}
	for k, v := range key.Metadata {
		if mv := existingMetadata[k]; mv == nil || *mv != *v {
			needsUpdate = true
			key.Metadata[k] = v
		}
	}
	return needsUpdate
}
