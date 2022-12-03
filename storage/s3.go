package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/ricochet1k/buildbuildbuild/utils/s3utils"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func NewS3Storage(sess *session.Session, bucket string, bufPool *utils.BetterPool[*bytes.Buffer]) (*S3Storage, error) {
	s3api := s3.New(sess)
	// list objects as a quick test to make sure S3 works
	_, err := s3api.ListObjects(&s3.ListObjectsInput{
		Bucket:  &bucket,
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}

	return &S3Storage{s3: s3api, bucket: bucket, bufPool: bufPool}, nil
}

type S3Storage struct {
	s3      s3iface.S3API
	bucket  string
	bufPool *utils.BetterPool[*bytes.Buffer]
}

func (s *S3Storage) MaybeUpdateMetadata(ctx context.Context, key BlobKey, existingMetadata Metadata) error {
	if key.NeedsMetadataUpdate(existingMetadata) {
		s3key := key.InstancePath()
		_, err := s.s3.CopyObjectWithContext(context.TODO(), &s3.CopyObjectInput{
			Bucket:     &s.bucket,
			Key:        &s3key,
			CopySource: aws.String(fmt.Sprintf("%v/%v", s.bucket, key)),
			Metadata:   key.Metadata,
		})
		return err
	}
	return nil
}

func (c *S3Storage) Exists(ctx context.Context, key BlobKey) (Metadata, error) {
	// DownloadReader offset=-1 already does HeadObject
	_, md, err := c.DownloadReader(ctx, key, -1, 1)
	return md, err
}

func (s *S3Storage) DownloadReader(ctx context.Context, key BlobKey, offset, limit int) (io.ReadCloser, Metadata, error) {
	s3key := key.InstancePath()

	var body io.ReadCloser
	var metadata Metadata

	if offset < 0 { // just check if it exists
		out, err := s.s3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: &s.bucket,
			Key:    &s3key,
		})
		if err != nil {
			// logrus.Warnf("S3 Doesn't Exist: %v", s3key)
			return nil, nil, utils.ErrorToStatus(err).Err()
		}

		if key.Size >= 0 && key.Compressor == 0 && *out.ContentLength != int64(key.Size) {
			logrus.Warnf("S3 Wrong size! %q  %v != %v", s3key, *out.ContentLength, int64(key.Size))
			s.s3.DeleteObject(&s3.DeleteObjectInput{
				Bucket: &s.bucket,
				Key:    &s3key,
			})
			return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("S3 Wrong size! %q  %v != %v", s3key, *out.ContentLength, int64(key.Size)))
		}

		// logrus.Infof("S3 Exists: %v", s3key)
		metadata = out.Metadata
	} else {
		// logrus.Infof("S3 DownloadReader %v", s3key)

		var dlrange *string
		if offset > 0 || limit > 0 {
			brange := fmt.Sprintf("bytes=%v-", offset)
			if limit > 0 {
				brange += fmt.Sprint(offset + limit - 1)
			}
			dlrange = proto.String(brange)
		}

		out, err := s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: &s.bucket,
			Key:    &s3key,
			Range:  dlrange,
		})
		if err != nil {
			// logrus.Errorf("S3 DownloadReader %v: %v", s3key, err)
			return nil, nil, utils.ErrorToStatus(err).Err()
		}

		if key.Compressor == 0 && key.Size >= 0 {
			if limit <= 0 {
				limit = key.Size
			}

			if offset+limit > key.Size {
				logrus.Warnf("Truncating limit from: %v", limit)
				limit = key.Size - offset
				logrus.Warnf("Truncating limit to: %v", limit)
			}

			if *out.ContentLength != int64(limit) {
				logrus.Warnf("S3 Wrong size! %q  %v != %v. (%v)", s3key, *out.ContentLength, int64(limit), offset)
				s.s3.DeleteObject(&s3.DeleteObjectInput{
					Bucket: &s.bucket,
					Key:    &s3key,
				})
				return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("S3 Wrong size! %q  %v != %v. (%v)", s3key, *out.ContentLength, int64(limit), offset))
			}
		}

		body = out.Body
		metadata = out.Metadata
	}

	if (key.Metadata != nil && len(key.Metadata) > 0) || key.ExpiresMin > 0 {
		// since this function is called in Race, we don't want to cancel the metadata update
		// so we are not using the parent context (maybe we should detach instead?)
		go s.MaybeUpdateMetadata(context.TODO(), key, metadata)
	}

	return body, metadata, nil
}

func (s *S3Storage) DownloadBytes(ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error) {
	return downloadBytes(s, ctx, key)
}

func (s *S3Storage) UploadWriter(ctx context.Context, key BlobKey) (utils.WriteCloserFinish, error) {
	// logrus.Infof("S3 UploadWriter %v", key.InstancePath())
	size := key.Size
	if key.Compressor != remoteexecution.Compressor_IDENTITY {
		size = -1
	}
	w, err := s3utils.NewS3MultipartUploader(ctx, s.s3, s.bufPool, s.bucket, key.InstancePath(), size, key.Metadata)
	if err != nil {
		// logrus.Errorf("S3 UploadWriter %v ERROR: %v", key, err)
		return nil, err
	}

	return w, nil
}

func (s *S3Storage) UploadReader(ctx context.Context, key BlobKey, r io.ReadSeeker) error {
	// logrus.Infof("S3 UploadReader %v", key.InstancePath())
	_, err := s.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Body:   r,
		Bucket: &s.bucket,
		// ChecksumSHA256: &key.Digest,
		Key:      aws.String(key.InstancePath()),
		Metadata: key.Metadata,
	})
	return err
}

func (s *S3Storage) UploadBytes(ctx context.Context, key BlobKey, data []byte) error {
	return s.UploadReader(ctx, key, bytes.NewReader(data))
}
