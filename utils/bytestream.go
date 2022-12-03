package utils

import (
	"io"

	bytestreampb "google.golang.org/genproto/googleapis/bytestream"
)

type FakeWriterAt struct {
	w io.Writer
}

func (fw FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	return fw.w.Write(p)
}

type BSReadWriter struct {
	rs bytestreampb.ByteStream_ReadServer
}

func NewBSReadWriter(rs bytestreampb.ByteStream_ReadServer) *BSReadWriter {
	return &BSReadWriter{rs}
}

// Write implements io.Writer
func (rs BSReadWriter) Write(data []byte) (n int, err error) {
	err = rs.rs.Send(&bytestreampb.ReadResponse{Data: data})
	return len(data), err
}

type BSWriteWriter struct {
	wc     bytestreampb.ByteStream_WriteClient
	name   string
	offset int
}

func NewBSWriteWriter(wc bytestreampb.ByteStream_WriteClient, name string) *BSWriteWriter {
	return &BSWriteWriter{wc, name, 0}
}

// Write implements io.Writer
func (wc BSWriteWriter) Write(data []byte) (n int, err error) {
	req := bytestreampb.WriteRequest{
		WriteOffset: int64(wc.offset),
		FinishWrite: false,
		Data:        data,
	}
	if wc.offset == 0 {
		req.ResourceName = wc.name
	}
	err = wc.wc.Send(&req)
	wc.offset += len(data)
	return len(data), err
}

func (wc BSWriteWriter) Finish() error {
	// send one last write with FinishWrite = true
	req := bytestreampb.WriteRequest{
		WriteOffset: int64(wc.offset),
		FinishWrite: true,
	}
	if wc.offset == 0 {
		req.ResourceName = wc.name
	}
	err := wc.wc.Send(&req)
	if err != nil {
		return err
	}

	return nil
}

func (wc BSWriteWriter) Close() error {
	_, err := wc.wc.CloseAndRecv()
	return err
}

type BSReadReader struct {
	rc     bytestreampb.ByteStream_ReadClient
	unread []byte
}

func NewBSReadReader(rc bytestreampb.ByteStream_ReadClient, firstBytes []byte) *BSReadReader {
	return &BSReadReader{rc, firstBytes}
}

func (b *BSReadReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		var data []byte
		if len(b.unread) > 0 {
			data = b.unread
			b.unread = nil
		} else {
			msg, err := b.rc.Recv()
			if err != nil {
				return n, err
			}
			data = msg.GetData()
		}

		copy(p[n:], data)
		if n+len(data) >= len(p) {
			b.unread = data[len(p)-n:]
			return len(p), nil
		}
		n += len(data)
	}

	return
}

func (b *BSReadReader) Close() error {
	return b.rc.CloseSend()
}
