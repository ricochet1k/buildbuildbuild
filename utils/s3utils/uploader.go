package s3utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type S3MultipartUploader struct {
	ctx   context.Context
	s3api s3iface.S3API
	// bufPool *utils.BetterPool[*bytes.Buffer]
	bucket string
	size   int

	key                string
	uploadId           string
	minChunkSize       int
	eg                 *errgroup.Group
	egctx              context.Context
	part               int
	totalBytes         int
	buf                *bytes.Buffer
	completedPartsChan chan *completedPart
	allCompletedParts  chan []*s3.CompletedPart
	finished           bool
}

func assertCloser(u *S3MultipartUploader) io.WriteCloser { return u }

type completedPart struct {
	partBytes int
	number    int
	etag      *string
	copy      bool
}

func NewS3MultipartUploader(ctx context.Context, s3api s3iface.S3API, bufPool *utils.BetterPool[*bytes.Buffer], bucket, key string, size int, metadata map[string]*string) (*S3MultipartUploader, error) {
	upload, err := s3api.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		Metadata: metadata,
	})
	if err != nil {
		// logrus.Errorf("S3MU CreateMultipartUpload Error: %v\n", err)
		return nil, err
	}

	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(10)

	completedPartsChan := make(chan *completedPart, 1)
	allCompletedParts := make(chan []*s3.CompletedPart, 1)

	// since uploads can complete out of order, we collect the completed upload parts separately from actually uploading them
	go func() {
		start := time.Now()
		lastSpeedReport := time.Now()
		nextPart := 1
		totalBytes := 0     // all bytes received
		committedBytes := 0 // all contiguous bytes received
		pendingPartsMap := map[int]*completedPart{}
		completedParts := []*s3.CompletedPart{}
		for completedPart := range completedPartsChan {
			if !completedPart.copy {
				totalBytes += completedPart.partBytes
			}
			if completedPart.number == nextPart {
				for completedPart.number == nextPart {
					nextPart += 1
					completedParts = append(completedParts, &s3.CompletedPart{
						ETag:       completedPart.etag,
						PartNumber: aws.Int64(int64(completedPart.number)),
					})
					committedBytes += completedPart.partBytes

					var ok bool
					completedPart, ok = pendingPartsMap[nextPart]
					if !ok {
						break
					}
					delete(pendingPartsMap, nextPart)
				}

				// c.uploads.Store(resourceName, uploadStatus{
				// 	committedBytes: committedBytes,
				// 	complete:       false,
				// })
			} else {
				pendingPartsMap[completedPart.number] = completedPart
			}

			if time.Since(lastSpeedReport) > 10*time.Second {
				lastSpeedReport = time.Now()
				logrus.Infof("Uploading %v... %.1f committed  %.1f / %.1f MB  %.1f MB/s", nextPart, utils.MB(committedBytes), utils.MB(totalBytes), utils.MB(size), utils.MBPS(totalBytes, time.Since(start)))
			}
		}
		if len(pendingPartsMap) > 0 {
			logrus.Warnf("Missing some parts?! %v %v %v", nextPart, pendingPartsMap, completedParts)
		}
		if time.Since(start) > 5*time.Second {
			logrus.Infof("Upload complete: %v... %.1f / %.1f MB  %.1f MB/s", nextPart, utils.MB(totalBytes), utils.MB(size), utils.MBPS(totalBytes, time.Since(start)))
		}
		// logrus.Infof("Upload collector complete: %v / %v", totalBytes, committedBytes)
		allCompletedParts <- completedParts
		close(allCompletedParts)
	}()

	minChunkSize := 5 * 1024 * 1024 // S3 required minimum chunk size
	const maxParts = 10000 - 1
	if size > 0 && int(size/maxParts) > minChunkSize {
		minChunkSize = int(size / maxParts)
	}

	buf := bufPool.Get()

	return &S3MultipartUploader{
		ctx:   ctx,
		s3api: s3api,
		// bufPool:            bufPool,
		bucket:             bucket,
		key:                key,
		size:               size,
		uploadId:           *upload.UploadId,
		minChunkSize:       minChunkSize,
		eg:                 eg,
		egctx:              egctx,
		part:               1,
		buf:                buf,
		completedPartsChan: completedPartsChan,
		allCompletedParts:  allCompletedParts,
	}, nil
}

func (u *S3MultipartUploader) Write(data []byte) (int, error) {
	err := u.egctx.Err()
	if err != nil {
		return 0, err
	}

	_, _ = u.buf.Write(data)

	if u.buf.Len() > u.minChunkSize {
		u.uploadChunk()
	}

	return len(data), nil
}

func (u *S3MultipartUploader) uploadChunk() {
	// closure captures these copies instead of the vars we are still modifying
	thePart := u.part
	u.part += 1
	theBuf := u.buf
	u.buf = &bytes.Buffer{} //u.bufPool.Get()

	u.totalBytes += theBuf.Len()

	// logrus.Infof("   S3MU UploadPart %q %v %v (total %v)\n", u.key, thePart, theBuf.Len(), u.totalBytes)
	u.eg.Go(func() error {
		// defer u.bufPool.Put(theBuf)
		partOutput, err := u.s3api.UploadPartWithContext(u.egctx, &s3.UploadPartInput{
			Bucket:     &u.bucket,
			Key:        &u.key,
			UploadId:   &u.uploadId,
			PartNumber: aws.Int64(int64(thePart)),
			Body:       bytes.NewReader(theBuf.Bytes()),
		})
		if err != nil {
			return err
		}
		u.completedPartsChan <- &completedPart{
			partBytes: theBuf.Len(),
			etag:      partOutput.ETag,
			number:    thePart,
		}
		return nil
	})
}

func (u *S3MultipartUploader) Finish() error {
	u.finished = true
	return nil
}

func (u *S3MultipartUploader) Close() error {
	// defer u.bufPool.Put(u.buf)

	if u.buf.Len() >= 0 {
		u.uploadChunk()
	}

	if err := u.eg.Wait(); err != nil {
		// logrus.Infof("   S3MU Upload Wait Err: %v %v\n", u.totalBytes, err)
		return err
	}

	close(u.completedPartsChan)
	completedParts := <-u.allCompletedParts

	if u.finished && (u.size < 0 || u.totalBytes == u.size) {
		// logrus.Infof("   S3MU Complete %q %v\n", u.key, u.totalBytes)

		_, err := u.s3api.CompleteMultipartUploadWithContext(u.ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   &u.bucket,
			Key:      &u.key,
			UploadId: &u.uploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
		if err != nil {
			// logrus.Errorf("   S3MU Complete Err %v\n", err)
			// logrus.Errorf(" %v %# v\n", len(completedParts), completedParts)
			return err
		}
	} else {
		logrus.Errorf("   S3MU Incomplete %v, %v == %v\n", u.finished, u.totalBytes, u.size)

		_, err := u.s3api.AbortMultipartUploadWithContext(u.ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &u.bucket,
			Key:      &u.key,
			UploadId: &u.uploadId,
		})
		if err != nil {
			// logrus.Errorf("   S3MU Abort Err %v\n", err)
			return err
		}

		return fmt.Errorf("Incomplete upload: %v / %v", u.totalBytes, u.size)
	}

	return nil
}
