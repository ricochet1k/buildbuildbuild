package server

import (
	"context"

	"github.com/bazelbuild/remote-apis/build/bazel/semver"

	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// GetCapabilities returns the server capabilities configuration of the
// remote endpoint.
// Only the capabilities of the services supported by the endpoint will
// be returned:
//   - Execution + CAS + Action Cache endpoints should return both
//     CacheCapabilities and ExecutionCapabilities.
//   - Execution only endpoints should return ExecutionCapabilities.
//   - CAS + Action Cache only endpoints should return CacheCapabilities.
//
// There are no method-specific errors.
func (c *Server) GetCapabilities(ctx context.Context, req *execpb.GetCapabilitiesRequest) (*execpb.ServerCapabilities, error) {
	// fmt.Printf("GetCapabilities: %q\n", req.InstanceName)
	return &execpb.ServerCapabilities{
		CacheCapabilities: &execpb.CacheCapabilities{
			DigestFunctions:                 []execpb.DigestFunction_Value{execpb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities:   &execpb.ActionCacheUpdateCapabilities{UpdateEnabled: true},
			CachePriorityCapabilities:       &execpb.PriorityCapabilities{},
			MaxBatchTotalSizeBytes:          1024 * 100, // 100kb
			SymlinkAbsolutePathStrategy:     execpb.SymlinkAbsolutePathStrategy_UNKNOWN,
			SupportedCompressors:            []execpb.Compressor_Value{execpb.Compressor_ZSTD},
			SupportedBatchUpdateCompressors: []execpb.Compressor_Value{execpb.Compressor_ZSTD},
		},
		ExecutionCapabilities: &execpb.ExecutionCapabilities{
			DigestFunction: execpb.DigestFunction_SHA256,
			ExecEnabled:    true,
			ExecutionPriorityCapabilities: &execpb.PriorityCapabilities{
				Priorities: []*execpb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 100,
					},
				},
			},
			SupportedNodeProperties: []string{},
		},
		DeprecatedApiVersion: &semver.SemVer{Major: 2},
		LowApiVersion:        &semver.SemVer{Major: 2},
		HighApiVersion:       &semver.SemVer{Major: 2},
	}, nil
}
