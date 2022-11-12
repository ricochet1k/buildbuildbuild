package server

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hashicorp/serf/serf"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
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
	config                  *Config
	bucket                  string
	sess                    *session.Session
	uploader                *s3manager.Uploader
	downloader              *s3manager.Downloader
	downloaderNoConcurrency *s3manager.Downloader
	uploads                 sync.Map
	metadata                []byte
	list                    *serf.Serf
	nodeState               map[string]*clusterpb.NodeState
	pubsub                  PubSub
	downloadConcurrency     chan struct{} // semaphore channel pattern
	jobSlots                chan struct{} // semaphore channel pattern
	downloading             sync.Map
	jobsRunning             sync.Map
	jobQueueMutex           sync.Mutex
	jobQueue                []func(string) // node name
	grpcClients             map[string]*grpc.ClientConn
}

type uploadStatus struct {
	committedBytes int64
	complete       bool
}

func NewServer(config *Config, sess *session.Session) (*Server, error) {
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)
	downloaderNoConcurrency := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) { d.Concurrency = 1 })

	// list objects as a quick test to make sure S3 works
	_, err := downloader.S3.ListObjects(&s3.ListObjectsInput{
		Bucket:  &config.Bucket,
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}

	c := &Server{
		config:                  config,
		bucket:                  config.Bucket,
		sess:                    sess,
		uploader:                uploader,
		downloader:              downloader,
		downloaderNoConcurrency: downloaderNoConcurrency,
		downloadConcurrency:     make(chan struct{}, config.DownloadConcurrency),
		nodeState:               map[string]*clusterpb.NodeState{},
		pubsub: PubSub{
			subscribers: map[string][]Subscriber{},
		},
		grpcClients: map[string]*grpc.ClientConn{},
	}

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

const (
	METADATA_EXPIRES string = "BBB-Expires"
)

const (
	CONTENT_LOG    string = "log"
	CONTENT_ACTION string = "action"
	CONTENT_CAS    string = "cas"
	CONTENT_ASSET  string = "asset"
)

// kind: cas/action/log
func StorageKey(instanceName, kind, suffix string) string {
	return fmt.Sprintf("%v/%v/%v", instanceName, kind, suffix)
}

func DigestKey(digest *execpb.Digest) string {
	return digest.Hash + ":" + fmt.Sprint(digest.SizeBytes)
}
