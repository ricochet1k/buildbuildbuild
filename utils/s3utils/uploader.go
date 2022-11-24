package s3utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type S3MultipartUploader struct {
	ctx     context.Context
	s3api   s3iface.S3API
	bufPool *sync.Pool
	bucket  string
	key     string
	size    int

	uploadingKey       string
	uploadId           string
	minChunkSize       int
	eg                 *errgroup.Group
	egctx              context.Context
	part               int
	totalBytes         int
	buf                *bytes.Buffer
	completedPartsChan chan *completedPart
	allCompletedParts  chan []*s3.CompletedPart
}

func assertWriter(u *S3MultipartUploader) io.WriteSeeker { return u }
func assertCloser(u *S3MultipartUploader) io.WriteCloser { return u }

type completedPart struct {
	partBytes int
	number    int
	etag      *string
	copy      bool
}

func NewS3MultipartUploader(ctx context.Context, s3api s3iface.S3API, bufPool *sync.Pool, bucket, key string, size int) (*S3MultipartUploader, error) {
	uploadingKey := "uploading/" + key

	upload, err := s3api.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &uploadingKey,
	})
	if err != nil {
		logrus.Errorf("BS CreateMultipartUpload Error: %v\n", err)
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
		allCompletedParts <- completedParts
		close(allCompletedParts)
	}()

	minChunkSize := 5 * 1024 * 1024 // S3 required minimum chunk size
	const maxParts = 10000 - 1
	if int(size/maxParts) > minChunkSize {
		minChunkSize = int(size / maxParts)
	}

	buf := bufPool.Get().(*bytes.Buffer)

	return &S3MultipartUploader{
		ctx:                ctx,
		s3api:              s3api,
		bufPool:            bufPool,
		bucket:             bucket,
		key:                key,
		size:               size,
		uploadingKey:       uploadingKey,
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

func (u *S3MultipartUploader) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 {
		thePart := u.part
		u.part += 1
		logrus.Infof("   BS UploadPartCopy %q %v\n", u.key, whence)
		u.eg.Go(func() error {
			partOutput, err := u.s3api.UploadPartCopyWithContext(u.ctx, &s3.UploadPartCopyInput{
				Bucket:          &u.bucket,
				Key:             &u.uploadingKey,
				UploadId:        &u.uploadId,
				PartNumber:      aws.Int64(int64(thePart)),
				CopySource:      aws.String(fmt.Sprintf("%v/%v", u.bucket, u.uploadingKey)),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%v", whence)),
			})
			if err != nil {
				return err
			}
			u.completedPartsChan <- &completedPart{
				partBytes: whence,
				etag:      partOutput.CopyPartResult.ETag,
				number:    thePart,
				copy:      true,
			}
			return nil
		})
	} else {
		panic("Wrong use of Seek in S3MultipartUploader")
	}

	return int64(whence), nil
}

func (u *S3MultipartUploader) Write(data []byte) (int, error) {
	err := u.egctx.Err()
	if err != nil {
		return 0, err
	}

	_, _ = u.buf.Write(data)

	if u.buf.Len() >= u.minChunkSize {
		u.uploadChunk()
	}

	return len(data), nil
}

func (u *S3MultipartUploader) uploadChunk() {
	// closure captures these copies instead of the vars we are still modifying
	thePart := u.part
	u.part += 1
	theBuf := u.buf
	u.buf = u.bufPool.Get().(*bytes.Buffer)

	u.totalBytes += theBuf.Len()

	// logrus.Infof("   BS UploadPart %q %v %v\n", resourceName, part, len(theData))
	u.eg.Go(func() error {
		defer func() {
			theBuf.Reset()
			u.bufPool.Put(theBuf)
		}()
		partOutput, err := u.s3api.UploadPartWithContext(u.egctx, &s3.UploadPartInput{
			Bucket:     &u.bucket,
			Key:        &u.uploadingKey,
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

func (u *S3MultipartUploader) Close() error {
	if u.buf.Len() >= 0 {
		u.uploadChunk()
	}

	if err := u.eg.Wait(); err != nil {
		logrus.Infof("   BS Upload Wait Err: %v %v\n", u.totalBytes, err)
		return err
	}

	close(u.completedPartsChan)
	completedParts := <-u.allCompletedParts

	// logrus.Infof("   BS Complete %q %v\n", resourceName, totalBytes)

	// rather than aborting if the upload didn't finish, we try to complete it anyway since bazel
	// might try to resume and we are writing to an "uploading" key
	_, err := u.s3api.CompleteMultipartUploadWithContext(u.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &u.bucket,
		Key:      &u.uploadingKey,
		UploadId: &u.uploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		logrus.Infof("   BS Complete Err %v\n", err)
		return err
	}

	if u.totalBytes == u.size {
		_, err = u.s3api.CopyObjectWithContext(u.ctx, &s3.CopyObjectInput{
			Bucket:     &u.bucket,
			CopySource: aws.String(fmt.Sprintf("%v/%v", u.bucket, u.uploadingKey)),
			Key:        &u.key,
		})
		if err != nil {
			logrus.Infof("   BS Complete Copy Err %v\n", err)
			return err
		}
	} else {
		return fmt.Errorf("Incomplete upload: %v / %v", u.totalBytes, u.size)
	}

	return nil
}
