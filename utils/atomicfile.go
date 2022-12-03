package utils

import (
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/sirupsen/logrus"
)

type WriteCloserFinish interface {
	io.WriteCloser
	// Unlike Close, this should be called to say that the write is complete and executed successfully
	// If you don't call Finish before calling Close, the file will be deleted
	Finish() error
}

type WriterAtCloserFinish interface {
	WriteCloserFinish
	io.WriterAt
}

func CreateAtomic(path string) (WriterAtCloserFinish, error) {
	tempPath := fmt.Sprintf("%v.pending%v", path, rand.Intn(4096))
	f, err := os.Create(tempPath)
	if err != nil {
		return nil, err
	}
	return &atomicFileWriteCloserFinish{
		path:      tempPath,
		finalPath: path,
		File:      f,
	}, nil
}

type atomicFileWriteCloserFinish struct {
	path      string
	finalPath string
	*os.File
	finished bool
}

func (f *atomicFileWriteCloserFinish) Finish() error {
	if DebugFilter(f.path) {
		logrus.Debugf("Atomic Finish: %v -> %v", f.path, f.finalPath)
	}
	if err := os.Rename(f.path, f.finalPath); err != nil {
		return err
	}
	f.finished = true
	return nil
}

func (f *atomicFileWriteCloserFinish) Close() error {
	err := f.File.Close()
	if !f.finished {
		if DebugFilter(f.finalPath) {
			logrus.Debugf("Atomic Close Incomplete: %v", f.path)
		}
		if err := os.Remove(f.path); err != nil {
			return err
		}
	} else {
		if DebugFilter(f.finalPath) {
			logrus.Debugf("Atomic Close: %v", f.path)
		}
	}
	return err
}
