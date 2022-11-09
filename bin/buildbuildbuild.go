package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/ricochet1k/buildbuildbuild/server"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	"google.golang.org/grpc"
)

var listen = flag.String("listen", "127.0.0.1", "listen host")
var port = flag.Int("port", 1234, "listen port")

var clusterName = flag.String("cluster_name", "", "name of node in cluster")
var clusterHost = flag.String("cluster_host", "127.0.0.1", "cluster host")
var clusterPort = flag.Int("cluster_port", 7946, "cluster port")
var join = flag.String("join", "", "join cluster ip")

var bucket = flag.String("bucket", "", "S3 bucket")
var region = flag.String("region", "us-east-1", "AWS region")
var worker = flag.Bool("worker", false, "Run worker node")

func main() {
	flag.Parse()
	address := fmt.Sprintf("%v:%v", *listen, *port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*region),
	}))

	nodeType := clusterpb.NodeType_SERVER
	if *worker {
		nodeType = clusterpb.NodeType_WORKER
	}

	c, err := server.NewServer(*bucket, sess, nodeType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to list objects in bucket %q: %v\n", *bucket, err)
		os.Exit(10)
	}
	c.Register(grpcServer)
	c.InitCluster(*clusterName, *clusterHost, *clusterPort, *join)

	fmt.Printf("Listening for GRPC on %s\n", address)
	grpcServer.Serve(lis)
}
