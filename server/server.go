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
	bucket        string
	uploader      *s3manager.Uploader
	downloader    *s3manager.Downloader
	uploads       *sync.Map
	nodeType      clusterpb.NodeType
	metadata      []byte
	list          *serf.Serf
	nodeState     map[string]*clusterpb.NodeState
	pubsub        PubSub
	jobSlots      chan struct{} // semaphore channel pattern
	jobQueueMutex sync.Mutex
	jobQueue      []func(string) // node name
}

type uploadStatus struct {
	committedBytes int64
	complete       bool
}

func NewServer(bucket string, sess *session.Session, nodeType clusterpb.NodeType) (*Server, error) {
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)

	// list objects as a quick test to make sure S3 works
	_, err := downloader.S3.ListObjects(&s3.ListObjectsInput{
		Bucket:  &bucket,
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}

	c := &Server{
		bucket:     bucket,
		uploader:   uploader,
		downloader: downloader,
		uploads:    &sync.Map{},
		nodeType:   nodeType,
		nodeState:  map[string]*clusterpb.NodeState{},
		pubsub: PubSub{
			subscribers: map[string][]Subscriber{},
		},
	}

	if nodeType == clusterpb.NodeType_WORKER {
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
