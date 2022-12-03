package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/ricochet1k/buildbuildbuild/utils"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/sirupsen/logrus"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type CacheData struct {
	myName       string
	members      []string
	memberHashes []uint64
}

func (c *CacheData) ClusterMembersUpdated(myName string, members []string) {
	logrus.Infof("Cluster members: %v %v", myName, members)
	c.myName = myName
	hashes := make([]uint64, len(members))
	for i, member := range members {
		hashes[i] = fnv1a.HashString64(member)
	}
	c.members = members
	c.memberHashes = hashes
}

// Uses Rendezvous Hashing to pick the cluster member responsible for this key
func (c *CacheData) PickCacheOwner(key BlobKey) string {
	if len(c.members) <= 1 {
		return c.myName
	}

	keyHash := fnv1a.HashString64(key.InstanceName)
	keyHash = fnv1a.AddString64(keyHash, key.Filename())
	bestI := 0
	bestHash := uint64(0)
	for i, memberHash := range c.memberHashes {
		hash := fnv1a.AddUint64(memberHash, keyHash)
		if hash > bestHash {
			bestI = i
			bestHash = hash
		}
	}
	return c.members[bestI]
}

type Server interface {
	ConnectToMember(name string) (*grpc.ClientConn, error)
}

type CacheStorage struct {
	Local   Storage
	Server  Server
	BufPool *utils.BetterPool[*bytes.Buffer]
	*CacheData
}

func (c *CacheStorage) Exists(ctx context.Context, key BlobKey) (Metadata, error) {
	r, md, err := c.DownloadReader(ctx, key, -1, 1)
	if err == nil && r != nil {
		r.Close()
	}
	return md, err
}

// Try to download from the cluster cache
func (c *CacheStorage) DownloadReader(ctx context.Context, key BlobKey, offset, limit int) (io.ReadCloser, Metadata, error) {
	member := c.PickCacheOwner(key)
	if utils.DebugFilter(key.Filename()) {
		logrus.Debugf("(%v) Cache downloading %v (%v bytes) from %v (%v, %v)", c.CacheData.myName, key.Filename(), key.Size, member, offset, limit)
	}
	if member == c.CacheData.myName {
		return c.Local.DownloadReader(ctx, key, offset, limit)
	}

	conn, err := c.Server.ConnectToMember(member)
	if err != nil {
		return nil, nil, err
	}

	bsclient := bytestream.NewByteStreamClient(conn)

	ctx = metadata.NewOutgoingContext(ctx, key.RequestMetadata())

	read, err := bsclient.Read(ctx, &bytestream.ReadRequest{
		ResourceName: key.ByteStreamReadKey(),
		ReadOffset:   int64(offset),
		ReadLimit:    int64(limit),
	})
	if err != nil {
		return nil, nil, err
	}

	md, err := read.Header()
	if err != nil {
		// logrus.Warnf("Cache DownloadReader Header err: %v", err)
		read.CloseSend()
		return nil, nil, err
	}
	key.SetMetadataFromRequest(md, nil)

	// no need to write updated metadata here, the underlying storage will have done that

	// this reader doesn't make sure it really can read until the first read call
	msg, err := read.Recv()
	if err != nil {
		// logrus.Warnf("Cache DownloadReader Read err: %v", err)
		read.CloseSend()
		return nil, key.Metadata, err
	}

	return utils.NewBSReadReader(read, msg.Data), key.Metadata, nil
}

func (c *CacheStorage) DownloadBytes(ctx context.Context, key BlobKey) (*bytes.Buffer, Metadata, error) {
	return downloadBytes(c, ctx, key)
}

func (c *CacheStorage) UploadWriter(ctx context.Context, key BlobKey) (utils.WriteCloserFinish, error) {
	member := c.PickCacheOwner(key)
	if utils.DebugFilter(key.Filename()) {
		logrus.Debugf("(%v) Cache uploading %v bytes to %v", c.CacheData.myName, key.Size, member)
	}
	if member == c.CacheData.myName {
		return c.Local.UploadWriter(ctx, key)
	}

	conn, err := c.Server.ConnectToMember(c.PickCacheOwner(key))
	if err != nil {
		return nil, err
	}

	bsclient := bytestream.NewByteStreamClient(conn)

	ctx = metadata.NewOutgoingContext(ctx, key.RequestMetadata())

	write, err := bsclient.Write(ctx)
	if err != nil {
		return nil, err
	}

	return utils.NewBSWriteWriter(write, key.ByteStreamWriteKey()), nil
}

// Upload to the cluster cache
func (c *CacheStorage) UploadBytes(ctx context.Context, key BlobKey, data []byte) error {
	w, err := c.UploadWriter(ctx, key)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err == nil {
		err = w.Finish()
	}
	if err != nil {
		w.Close()
		return err
	}

	return w.Close()
}
