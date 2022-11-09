package server

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	execpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
)

func (c *Server) InitWorker() {
	c.jobSlots = make(chan struct{}, 4)
	go func() {
		time.Sleep(5 * time.Second)
		c.SendState(&clusterpb.NodeState{
			JobsRunning:   int32(len(c.jobSlots)),
			JobsSlotsFree: int32(cap(c.jobSlots)) - int32(len(c.jobSlots)),
		})
	}()
}

func (c *Server) DownloadProto(ctx context.Context, key string, msg proto.Message) error {
	var b aws.WriteAtBuffer
	_, err := c.downloader.DownloadWithContext(ctx, &b, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b.Bytes(), msg); err != nil {
		return err
	}

	return nil
}

func (c *Server) StartJob(job *clusterpb.StartJob) error {
	select {
	case c.jobSlots <- struct{}{}:
		defer func() { <-c.jobSlots }()
	default:
		return fmt.Errorf("Cannot start job %q, slots full", job.Id)
	}

	ctx := context.TODO()

	fmt.Printf("Worker starting job: %v\n", job.Id)

	c.Publish("jobstatus:"+job.Id, &clusterpb.Message{
		Type: &clusterpb.Message_JobStatus{
			JobStatus: &clusterpb.JobStatus{
				Stage: execpb.ExecutionStage_CACHE_CHECK,
			},
		},
	})

	var action execpb.Action
	if err := c.DownloadProto(ctx, StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(job.ActionDigest)), &action); err != nil {
		fmt.Printf("Worker cannot get action: %v\n", err)
	}

	var inputRoot execpb.Directory
	if err := c.DownloadProto(ctx, StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(action.InputRootDigest)), &inputRoot); err != nil {
		fmt.Printf("Worker cannot get inputRoot: %v\n", err)
	}

	var command execpb.Command
	if err := c.DownloadProto(ctx, StorageKey(job.InstanceName, CONTENT_CAS, DigestKey(action.CommandDigest)), &command); err != nil {
		fmt.Printf("Worker cannot get command: %v\n", err)
	}

	c.Publish("jobstatus:"+job.Id, &clusterpb.Message{
		Type: &clusterpb.Message_JobStatus{
			JobStatus: &clusterpb.JobStatus{
				Stage: execpb.ExecutionStage_COMPLETED,
			},
		},
	})

	return nil
}
