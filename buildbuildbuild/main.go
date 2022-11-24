package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/hashicorp/go-sockaddr/template"
	"github.com/ricochet1k/buildbuildbuild/server"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var listen = flag.String("listen", "{{ GetPrivateIP }}", "listen host")
var port = flag.Int("port", 1234, "listen port")

var nodeName = flag.String("node_name", "", "name of node in cluster")
var bindHost = flag.String("bind_host", "{{ GetPrivateIP }}", "cluster bind host (go-sockaddr template)")
var bindPort = flag.Int("bind_port", 7946, "cluster bind port")
var advertiseHost = flag.String("advertise_host", "{{ GetPrivateIP }}", "advertise host (go-sockaddr template)")
var advertisePort = flag.Int("advertise_port", 0, "advertise port")
var autojoin = flag.String("autojoin", "default_cluster", "autojoin cluster details stored in S3 by this key")
var join = flag.String("join", "", "join cluster ips")

var bucket = flag.String("bucket", "", "S3 bucket")
var region = flag.String("region", "us-east-1", "AWS region")
var workerSlots = flag.Int("worker_slots", 0, "Run worker node with concurrent jobs")
var downloadConcurrency = flag.Int("download_concurrency", 10, "How many concurrent file downloads")

var noCleanupExecroot = flag.String("no_cleanup_execroot", "", "Don't delete execroot on (fail|all)")

var cacheDir = flag.String("cache_dir", "", "Download cache")

func main() {
	log.SetOutput(logrus.New().Writer())
	flag.Parse()

	if *advertisePort == 0 {
		*advertisePort = *bindPort
	}
	if *cacheDir == "" {
		*cacheDir = filepath.Join(os.TempDir(), "bbb_cache")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &server.Config{
		Listen:              *listen,
		Port:                *port,
		NodeName:            *nodeName,
		BindHost:            *bindHost,
		BindPort:            *bindPort,
		AdvertiseHost:       *advertiseHost,
		AdvertisePort:       *advertisePort,
		Autojoin:            *autojoin,
		Join:                *join,
		Bucket:              *bucket,
		Region:              *region,
		WorkerSlots:         *workerSlots,
		DownloadConcurrency: *downloadConcurrency,
		CacheDir:            *cacheDir,
		NoCleanupExecroot:   *noCleanupExecroot,
	}

	err := os.MkdirAll(config.CacheDir, fs.FileMode(0755))
	if err != nil && !os.IsExist(err) {
		logrus.Fatalf("Could not mkdir cache %q, please set -cache_dir: %v", config.CacheDir, err)
	}

	listenHost, err := template.Parse(*listen)
	if err != nil {
		logrus.Fatalf("Failed to parse listen %q: %v", *listen, err)
	}

	address := fmt.Sprintf("%v:%v", listenHost, *port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*region),
	}))

	c, err := server.NewServer(ctx, config, sess)
	if err != nil {
		logrus.Fatalf("Unable to list objects in bucket %q: %v\n", *bucket, err)
	}
	c.Register(grpcServer)
	c.InitCluster()

	// logrus.Printf("Listening for GRPC on %s\n", address)
	// err = grpcServer.Serve(lis)
	// if err != nil {
	// 	logrus.Fatalf("could not serve: %v", err)
	// }

	h2server := &http2.Server{
		// MaxHandlers:                  0,
		// MaxConcurrentStreams:         0,
		// MaxReadFrameSize:             0,
		// PermitProhibitedCipherSuites: false,
		// IdleTimeout:                  0,
		// MaxUploadBufferPerConnection: 0,
		// MaxUploadBufferPerStream:     0,
		// NewWriteScheduler: func() http2.WriteScheduler {
		// },
		// CountError: func(errType string) {
		// },
	}

	h1server := &http.Server{
		Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 && strings.HasPrefix(
				r.Header.Get("Content-Type"), "application/grpc") {
				grpcServer.ServeHTTP(w, r)
			} else {
				http.DefaultServeMux.ServeHTTP(w, r)
			}
		}), h2server),
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	eg, _ := errgroup.WithContext(context.Background())
	go func() {
		s := <-sigCh
		logrus.Printf("got signal %v, attempting graceful shutdown", s)
		cancel()

		shutdownctx, shutdowncancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdowncancel()
		eg.Go(func() error {
			c.GracefulStop()
			return nil
		})
		// this isn't supported when using grpcServer.ServeHTTP
		// eg.Go(func() error {
		// 	grpcServer.GracefulStop()
		// 	return nil
		// })
		eg.Go(func() error {
			return h1server.Shutdown(shutdownctx)
		})
	}()

	logrus.Infof("Serving at %v", lis.Addr())
	if err = h1server.Serve(lis); err != nil {
		logrus.Errorf("Serve: %v", err)
	}

	if err := eg.Wait(); err != nil {
		logrus.Errorf("Shutdown error: %v", err)
	} else {
		logrus.Infoln("Graceful shutdown")
	}
}
