package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/ricochet1k/buildbuildbuild/server"
	"github.com/sirupsen/logrus"
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
	log.SetOutput(logrus.New().Writer())
	flag.Parse()

	config := &server.Config{
		Listen:      *listen,
		Port:        *port,
		ClusterName: *clusterName,
		ClusterHost: *clusterHost,
		ClusterPort: *clusterPort,
		Join:        *join,
		Bucket:      *bucket,
		Region:      *region,
		Worker:      *worker,
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := fmt.Sprintf("%v:%v", *listen, *port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*region),
	}))

	c, err := server.NewServer(config, sess)
	if err != nil {
		logrus.Fatalf("Unable to list objects in bucket %q: %v\n", *bucket, err)
	}
	c.Register(grpcServer)
	c.InitCluster()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s := <-sigCh
		logrus.Printf("got signal %v, attempting graceful shutdown", s)
		cancel()
		wg.Add(1)
		go func() {
			c.GracefulStop()
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			grpcServer.GracefulStop()
			wg.Done()
		}()
		wg.Done()
	}()

	logrus.Printf("Listening for GRPC on %s\n", address)
	err = grpcServer.Serve(lis)
	if err != nil {
		logrus.Fatalf("could not serve: %v", err)
	}

	wg.Wait()
	logrus.Println("Graceful shutdown")
}
