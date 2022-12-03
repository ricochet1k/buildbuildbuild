package utils

import (
	"errors"
	"os"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ErrorToStatus(err error) *status.Status {
	var statuserr interface {
		GRPCStatus() *status.Status
	}
	if errors.As(err, &statuserr) {
		stold := statuserr.GRPCStatus()
		stproto := stold.Proto()
		stproto.Message = err.Error()
		return status.FromProto(stproto)
	}

	if st, ok := status.FromError(err); ok {
		return st
	}

	if st := status.FromContextError(err); st.Code() != codes.Unknown {
		return st
	}

	var aerr awserr.Error
	if errors.As(err, &aerr) && aerr.Code() == s3.ErrCodeNoSuchKey {
		return status.New(codes.NotFound, err.Error())
	}

	if errors.Is(err, os.ErrNotExist) {
		return status.New(codes.NotFound, err.Error())
	}

	return status.New(codes.Unknown, err.Error())
}
