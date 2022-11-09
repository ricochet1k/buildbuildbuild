package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	bytestreampb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FakeWriterAt struct {
	w io.Writer
}

func (fw FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	return fw.w.Write(p)
}

// `Read()` is used to retrieve the contents of a resource as a sequence
// of bytes. The bytes are returned in a sequence of responses, and the
// responses are delivered as the results of a server-side streaming RPC.
func (c *Server) Read(req *bytestreampb.ReadRequest, rs bytestreampb.ByteStream_ReadServer) error {
	fmt.Printf("ByteStream Downloading %q\n", req.ResourceName)

	re, wr := io.Pipe()
	var senderr error
	go func() {
		var buf [5 * 1024]byte
		for {
			n, err := re.Read(buf[:])
			if err != nil { // EOF or not, we need to close and return
				re.Close()
				return
			}
			err = rs.Send(&bytestreampb.ReadResponse{
				Data: buf[:n],
			})
			if err != nil {
				senderr = err
				re.CloseWithError(err)
				return
			}
		}
	}()

	var dlrange *string
	if req.ReadOffset > 0 || req.ReadLimit > 0 {
		dlrange = proto.String(fmt.Sprintf("bytes=%v-%v", req.ReadOffset, req.ReadOffset+req.ReadLimit))
	}

	_, err := c.downloader.DownloadWithContext(rs.Context(), FakeWriterAt{wr}, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &req.ResourceName,
		Range:  dlrange,
	})
	if senderr != nil {
		err = senderr
	}

	return err
}

// `Write()` is used to send the contents of a resource as a sequence of
// bytes. The bytes are sent in a sequence of request protos of a client-side
// streaming RPC.
//
// A `Write()` action is resumable. If there is an error or the connection is
// broken during the `Write()`, the client should check the status of the
// `Write()` by calling `QueryWriteStatus()` and continue writing from the
// returned `committed_size`. This may be less than the amount of data the
// client previously sent.
//
// Calling `Write()` on a resource name that was previously written and
// finalized could cause an error, depending on whether the underlying service
// allows over-writing of previously written resources.
//
// When the client closes the request channel, the service will respond with
// a `WriteResponse`. The service will not view the resource as `complete`
// until the client has sent a `WriteRequest` with `finish_write` set to
// `true`. Sending any requests on a stream after sending a request with
// `finish_write` set to `true` will cause an error. The client **should**
// check the `WriteResponse` it receives to determine how much data the
// service was able to commit and whether the service views the resource as
// `complete` or not.
func (c *Server) Write(ws bytestreampb.ByteStream_WriteServer) error {
	req, err := ws.Recv()
	if err != nil {
		return err
	}

	resourceName := req.ResourceName
	// fmt.Printf("ByteStream Uploading %q\n", resourceName)

	totalBytes := int64(0)
	c.uploads.Store(resourceName, uploadStatus{
		committedBytes: totalBytes,
		complete:       false,
	})
	go func() {
		time.Sleep(10 * time.Minute)
		c.uploads.Delete(resourceName)
	}()

	upload, err := c.uploader.S3.CreateMultipartUploadWithContext(ws.Context(), &s3.CreateMultipartUploadInput{
		Bucket: &c.bucket,
		Key:    &resourceName,
	})
	if err != nil {
		fmt.Printf(". BS CreateMultipartUpload Error: %v\n", err)
		return err
	}

	completed := false
	defer func() {
		if !completed {
			_, _ = c.uploader.S3.AbortMultipartUploadWithContext(ws.Context(), &s3.AbortMultipartUploadInput{
				Bucket:   &c.bucket,
				Key:      &resourceName,
				UploadId: upload.UploadId,
			})
		}
	}()

	part := int64(1)
	eg, ctx := errgroup.WithContext(ws.Context())
	eg.SetLimit(5)

	type completedPart struct {
		partBytes int64
		number    *int64
		etag      *string
	}

	completedPartsChan := make(chan *completedPart, 1)
	allCompletedParts := make(chan []*s3.CompletedPart, 1)
	go func() {
		nextPart := int64(1)
		totalBytes := int64(0)
		pendingPartsMap := map[int64]*completedPart{}
		completedParts := []*s3.CompletedPart{}
		for completedPart := range completedPartsChan {
			if *completedPart.number == nextPart {
				for *completedPart.number == nextPart {
					nextPart += 1
					completedParts = append(completedParts, &s3.CompletedPart{
						ETag:       completedPart.etag,
						PartNumber: completedPart.number,
					})
					totalBytes += completedPart.partBytes

					var ok bool
					completedPart, ok = pendingPartsMap[nextPart]
					if !ok {
						break
					}
					delete(pendingPartsMap, nextPart)
				}

				c.uploads.Store(resourceName, uploadStatus{
					committedBytes: totalBytes,
					complete:       false,
				})
			} else {
				pendingPartsMap[*completedPart.number] = completedPart
			}
		}
		if len(pendingPartsMap) > 0 {
			fmt.Printf("Missing some parts?! %v %v %v", nextPart, pendingPartsMap, completedParts)
		}
		allCompletedParts <- completedParts
		close(allCompletedParts)
	}()

	if req.WriteOffset > 0 {
		thePart := aws.Int64(part)
		part += 1
		fmt.Printf("   BS UploadPartCopy %q\n", resourceName)
		eg.Go(func() error {
			partOutput, err := c.uploader.S3.UploadPartCopyWithContext(ws.Context(), &s3.UploadPartCopyInput{
				Bucket:          &c.bucket,
				Key:             &resourceName,
				UploadId:        upload.UploadId,
				PartNumber:      thePart,
				CopySource:      aws.String(fmt.Sprintf("%v/%v", c.bucket, resourceName)),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%v", req.WriteOffset)),
			})
			if err != nil {
				return err
			}
			completedPartsChan <- &completedPart{
				partBytes: int64(len(req.Data)),
				etag:      partOutput.CopyPartResult.ETag,
				number:    thePart,
			}
			return nil
		})
	}

	var buf bytes.Buffer

	for {
		_, _ = buf.Write(req.Data)

		if req.FinishWrite || buf.Len() >= 5*1024*1024 {
			// closure captures these copies instead of the vars we are still modifying
			thePart := aws.Int64(part)
			part += 1
			theData := buf.Bytes()
			buf = bytes.Buffer{} // buf.Bytes is not a copy. We re-init buf so we don't overwrite while trying to send
			// TODO: Pool?

			totalBytes += int64(len(theData))

			// fmt.Printf("   BS UploadPart %q %v %v\n", resourceName, part, len(theData))
			eg.Go(func() error {
				partOutput, err := c.uploader.S3.UploadPartWithContext(ctx, &s3.UploadPartInput{
					Bucket:     &c.bucket,
					Key:        &resourceName,
					UploadId:   upload.UploadId,
					PartNumber: thePart,
					Body:       bytes.NewReader(theData),
				})
				if err != nil {
					return err
				}
				completedPartsChan <- &completedPart{
					partBytes: int64(len(theData)),
					etag:      partOutput.ETag,
					number:    thePart,
				}
				return nil
			})
		}

		if req.FinishWrite {
			break
		}

		req, err = ws.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Printf("   BS Upload Recv Err: %v %v\n", totalBytes, err)
			return err
		}
	}

	if err = eg.Wait(); err != nil {
		fmt.Printf("   BS Upload Wait Err: %v %v\n", totalBytes, err)
		return err
	}

	close(completedPartsChan)
	completedParts := <-allCompletedParts

	// fmt.Printf("   BS Complete %q %v\n", resourceName, totalBytes)
	_, err = c.uploader.S3.CompleteMultipartUploadWithContext(ws.Context(), &s3.CompleteMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      &resourceName,
		UploadId: upload.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		fmt.Printf("   BS Complete Err %v\n", err)
		return err
	}

	c.uploads.Store(resourceName, uploadStatus{
		committedBytes: totalBytes,
		complete:       true,
	})

	completed = true

	// fmt.Printf("   BS SendAndClose %q %v\n", resourceName, totalBytes)
	return ws.SendAndClose(&bytestreampb.WriteResponse{
		CommittedSize: totalBytes,
	})
}

// `QueryWriteStatus()` is used to find the `committed_size` for a resource
// that is being written, which can then be used as the `write_offset` for
// the next `Write()` call.
//
// If the resource does not exist (i.e., the resource has been deleted, or the
// first `Write()` has not yet reached the service), this method returns the
// error `NOT_FOUND`.
//
// The client **may** call `QueryWriteStatus()` at any time to determine how
// much data has been processed for this resource. This is useful if the
// client is buffering data and needs to know which data can be safely
// evicted. For any sequence of `QueryWriteStatus()` calls for a given
// resource name, the sequence of returned `committed_size` values will be
// non-decreasing.
func (c *Server) QueryWriteStatus(ctx context.Context, req *bytestreampb.QueryWriteStatusRequest) (*bytestreampb.QueryWriteStatusResponse, error) {
	value, ok := c.uploads.Load(req.ResourceName)
	if !ok {
		return nil, status.Error(codes.NotFound, "not found")
	}

	stat := value.(uploadStatus)

	// fmt.Printf("  QueryWriteStatus: %v %v\n", req.ResourceName, stat)

	return &bytestreampb.QueryWriteStatusResponse{
		CommittedSize: stat.committedBytes,
		Complete:      stat.complete,
	}, nil
}
