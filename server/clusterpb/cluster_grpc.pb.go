// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.9
// source: cluster.proto

package clusterpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// WorkerClient is the client API for Worker service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WorkerClient interface {
	StartJob(ctx context.Context, in *StartJobRequest, opts ...grpc.CallOption) (Worker_StartJobClient, error)
	FollowJob(ctx context.Context, in *FollowJobRequest, opts ...grpc.CallOption) (Worker_FollowJobClient, error)
}

type workerClient struct {
	cc grpc.ClientConnInterface
}

func NewWorkerClient(cc grpc.ClientConnInterface) WorkerClient {
	return &workerClient{cc}
}

func (c *workerClient) StartJob(ctx context.Context, in *StartJobRequest, opts ...grpc.CallOption) (Worker_StartJobClient, error) {
	stream, err := c.cc.NewStream(ctx, &Worker_ServiceDesc.Streams[0], "/buildbuildbuild.Worker/StartJob", opts...)
	if err != nil {
		return nil, err
	}
	x := &workerStartJobClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Worker_StartJobClient interface {
	Recv() (*JobStatus, error)
	grpc.ClientStream
}

type workerStartJobClient struct {
	grpc.ClientStream
}

func (x *workerStartJobClient) Recv() (*JobStatus, error) {
	m := new(JobStatus)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *workerClient) FollowJob(ctx context.Context, in *FollowJobRequest, opts ...grpc.CallOption) (Worker_FollowJobClient, error) {
	stream, err := c.cc.NewStream(ctx, &Worker_ServiceDesc.Streams[1], "/buildbuildbuild.Worker/FollowJob", opts...)
	if err != nil {
		return nil, err
	}
	x := &workerFollowJobClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Worker_FollowJobClient interface {
	Recv() (*JobStatus, error)
	grpc.ClientStream
}

type workerFollowJobClient struct {
	grpc.ClientStream
}

func (x *workerFollowJobClient) Recv() (*JobStatus, error) {
	m := new(JobStatus)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// WorkerServer is the server API for Worker service.
// All implementations must embed UnimplementedWorkerServer
// for forward compatibility
type WorkerServer interface {
	StartJob(*StartJobRequest, Worker_StartJobServer) error
	FollowJob(*FollowJobRequest, Worker_FollowJobServer) error
	mustEmbedUnimplementedWorkerServer()
}

// UnimplementedWorkerServer must be embedded to have forward compatible implementations.
type UnimplementedWorkerServer struct {
}

func (UnimplementedWorkerServer) StartJob(*StartJobRequest, Worker_StartJobServer) error {
	return status.Errorf(codes.Unimplemented, "method StartJob not implemented")
}
func (UnimplementedWorkerServer) FollowJob(*FollowJobRequest, Worker_FollowJobServer) error {
	return status.Errorf(codes.Unimplemented, "method FollowJob not implemented")
}
func (UnimplementedWorkerServer) mustEmbedUnimplementedWorkerServer() {}

// UnsafeWorkerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WorkerServer will
// result in compilation errors.
type UnsafeWorkerServer interface {
	mustEmbedUnimplementedWorkerServer()
}

func RegisterWorkerServer(s grpc.ServiceRegistrar, srv WorkerServer) {
	s.RegisterService(&Worker_ServiceDesc, srv)
}

func _Worker_StartJob_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StartJobRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WorkerServer).StartJob(m, &workerStartJobServer{stream})
}

type Worker_StartJobServer interface {
	Send(*JobStatus) error
	grpc.ServerStream
}

type workerStartJobServer struct {
	grpc.ServerStream
}

func (x *workerStartJobServer) Send(m *JobStatus) error {
	return x.ServerStream.SendMsg(m)
}

func _Worker_FollowJob_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FollowJobRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WorkerServer).FollowJob(m, &workerFollowJobServer{stream})
}

type Worker_FollowJobServer interface {
	Send(*JobStatus) error
	grpc.ServerStream
}

type workerFollowJobServer struct {
	grpc.ServerStream
}

func (x *workerFollowJobServer) Send(m *JobStatus) error {
	return x.ServerStream.SendMsg(m)
}

// Worker_ServiceDesc is the grpc.ServiceDesc for Worker service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Worker_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "buildbuildbuild.Worker",
	HandlerType: (*WorkerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StartJob",
			Handler:       _Worker_StartJob_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "FollowJob",
			Handler:       _Worker_FollowJob_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "cluster.proto",
}
