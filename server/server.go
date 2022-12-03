package server

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	"github.com/ricochet1k/buildbuildbuild/storage"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	assetpb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	logstreampb "github.com/bazelbuild/remote-apis/build/bazel/remote/logstream/v1"
	bytestreampb "google.golang.org/genproto/googleapis/bytestream"
	buildeventpb "google.golang.org/genproto/googleapis/devtools/build/v1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
)

type Server struct {
	clusterpb.UnimplementedWorkerServer
	config  *Config
	context context.Context
	bucket  string
	sess    *session.Session
	// uploader                *s3manager.Uploader
	// downloader              *s3manager.Downloader
	// downloaderNoConcurrency *s3manager.Downloader
	uploads             sync.Map
	metadata            []byte
	list                *serf.Serf
	nodeState           map[string]*clusterpb.NodeState
	pubsub              PubSub
	downloadConcurrency chan struct{} // semaphore channel pattern
	jobSlots            chan struct{} // semaphore channel pattern
	downloading         sync.Map
	jobsRunning         sync.Map
	jobQueueMutex       sync.Mutex
	jobQueue            []func(string) // node name
	grpcClientsMutex    sync.Mutex
	grpcClients         map[string]*grpc.ClientConn
	bufPool             utils.BetterPool[*bytes.Buffer]
	storage.CacheData
	storage.Storage
	LocalStorage  storage.Storage
	CacheStorage  storage.Storage
	RemoteStorage storage.Storage
}

func NewServer(ctx context.Context, config *Config, sess *session.Session) (*Server, error) {
	c := &Server{
		context:             ctx,
		config:              config,
		bucket:              config.Bucket,
		sess:                sess,
		downloadConcurrency: make(chan struct{}, config.DownloadConcurrency),
		nodeState:           map[string]*clusterpb.NodeState{},
		pubsub: PubSub{
			subscribers: map[string][]Subscriber{},
		},
		grpcClients: map[string]*grpc.ClientConn{},
		bufPool:     utils.NewPool(func() *bytes.Buffer { return new(bytes.Buffer) }, func(b *bytes.Buffer) { b.Reset() }),
	}

	c.LocalStorage = &storage.DiskStorage{
		Path:    config.CacheDir,
		BufPool: &c.bufPool,
	}
	c.CacheStorage = &storage.CacheStorage{
		Local:     c.LocalStorage,
		Server:    c,
		BufPool:   &c.bufPool,
		CacheData: &c.CacheData,
	}

	if config.Bucket != "" {
		s3store, err := storage.NewS3Storage(sess, config.Bucket, &c.bufPool)
		if err != nil {
			return nil, err
		}
		c.RemoteStorage = s3store

		c.Storage = &storage.ComboStorage{
			Cache: c.CacheStorage,
			Main:  s3store,
		}
	} else {
		c.Storage = c.CacheStorage
	}

	storageCompressor := execpb.Compressor_IDENTITY
	if config.Compress {
		storageCompressor = execpb.Compressor_ZSTD
	}
	c.Storage = storage.NewCompressed(c.Storage, storageCompressor)

	if config.WorkerSlots > 0 {
		c.InitWorker()
	}

	return c, nil
}

func (c *Server) Register(grpcServer *grpc.Server) {
	assetpb.RegisterFetchServer(grpcServer, c)
	assetpb.RegisterPushServer(grpcServer, c)
	execpb.RegisterContentAddressableStorageServer(grpcServer, c)
	execpb.RegisterActionCacheServer(grpcServer, c)
	execpb.RegisterCapabilitiesServer(grpcServer, c)
	execpb.RegisterExecutionServer(grpcServer, c)
	logstreampb.RegisterLogStreamServiceServer(grpcServer, c)
	buildeventpb.RegisterPublishBuildEventServer(grpcServer, c)
	bytestreampb.RegisterByteStreamServer(grpcServer, c)
	longrunningpb.RegisterOperationsServer(grpcServer, c)

	if c.config.WorkerSlots > 0 {
		clusterpb.RegisterWorkerServer(grpcServer, c)
	}
}

func (c *Server) CachePath(key storage.BlobKey) string {
	cachepath := filepath.Join(c.config.CacheDir, key.InstancePath())
	err := os.MkdirAll(filepath.Dir(cachepath), fs.FileMode(0755))
	if err != nil {
		logrus.Warnf("Unable to mkdir cache path %q: %v", cachepath, err)
	}
	return cachepath
}

func (c *Server) DownloadProto(ctx context.Context, key storage.BlobKey, msg proto.Message) error {
	key.Metadata.Put("proto", fmt.Sprintf("%T", msg))

	buf, _, err := c.DownloadBytes(ctx, key)
	if err != nil {
		return err
	}
	// defer c.bufPool.Put(buf)

	return proto.Unmarshal(buf.Bytes(), msg)
}

func (c *Server) UploadProto(ctx context.Context, key storage.BlobKey, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	key.Metadata.Put("proto", fmt.Sprintf("%T", msg))

	return c.UploadBytes(ctx, key, data)
}

// kind: cas/action/log
// func StorageKey(instanceName string, kind storage.ContentKind, suffix string) string {
// 	return fmt.Sprintf("%v/%v/%v", instanceName, kind, suffix)
// }

// func DigestKey(digest *execpb.Digest) string {
// 	return digest.Hash + ":" + fmt.Sprint(digest.SizeBytes)
// }
