package server

import (
	"context"
	"fmt"
	"time"

	logstreampb "github.com/bazelbuild/remote-apis/build/bazel/remote/logstream/v1"
)

// Create a LogStream which may be written to.
//
// The returned LogStream resource name will include a `write_resource_name`
// which is the resource to use when writing to the LogStream.
// Callers of CreateLogStream are expected to NOT publish the
// `write_resource_name`.
func (c *Server) CreateLogStream(ctx context.Context, req *logstreampb.CreateLogStreamRequest) (*logstreampb.LogStream, error) {
	name := fmt.Sprintf("logs/%v/%v", time.Now().Format("2006-01-02"), req.Parent)
	fmt.Printf("LogStream %q", name)
	return &logstreampb.LogStream{
		Name:              name,
		WriteResourceName: name, // todo: add auth token?
	}, nil
}
