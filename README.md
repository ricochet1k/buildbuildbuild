# buildbuildbuild

A Bazel Remote Build Cluster, designed for ease of deployment and minimal maintanence requirements. All you need is a bunch of machines that can connect to an S3 compatible bucket. They will self-assemble and automatically distribute the workload among themselves. Autoscale as needed!

Note: This is not production ready! Though it does function it hasn't been thoroughly tested for reliability. There is also no sandboxing whatsoever!

## Features
- [x] Remote Build API
- [x] Remote Asset API
- [x] S3 storage / cache
- [x] Streaming logs saved to S3
- [x] Local / Cluster cache
- [x] Autojoin by UDP broadcast
- [x] Autojoin by member list in S3
- [x] Compress in storage
- [x] Parallel download/upload
- [ ] Sandboxing
- [ ] Analytics
- [ ] Dashboard


## Configuration
`buildbuildbuild` is configured exclusively through command-line flags. All hostnames use [go-sockaddr](https://github.com/hashicorp/go-sockaddr) to parse, which makes it really easy to configure.

### Bazel remote config
- `--listen` - listen host, default: `{{ GetPrivateIP }}`
- `--port` - listen port, default: `1234`
- `--required_header` - Require a remote header sent by bazel, use with `bazel --remote_header=` or put in .bazelrc, default: none

### Cluster communication
Bind is what ip/port it actually tries to listen on, advertise is how it tells other nodes to connect to it.
So, for an example, it could bind to `0.0.0.0` to listen to all interfaces but must still advertise a specific IP so that
others can connect to it.
- `--cluster_name` - only join nodes with the same cluster name, default: `default_cluster`
- `--node_name` - name of node in cluster, default: ``
- `--bind_host` - cluster bind host (go-sockaddr), default: `{{ GetPrivateIP }}`
- `--bind_port` - cluster bind port, default: `7946`
- `--advertise_host` - advertise host (go-sockaddr), default: `{{ GetPrivateIP }}`
- `--advertise_port` - advertise port, default: same as `--bind_port`
- `--autojoin_s3` - store and autojoin member list in this s3 key, default: none
- `--autojoin_udp` - autojoin with UDP broadcast, default: `{{ GetPrivateInterfaces | attr "broadcast" }}:9876`
- `--join` - try to join cluster at the given address on startup, default: none
- `--secret_key` - cluster encryption key for all cluster-internal communication, all nodes must have the same key

### Storage
- `--bucket` - S3 bucket, required
- `--region` - S3 bucket AWS region, default: `us-east-1`
- `--cache_dir` - where to store blobs locally, required

### Other config
- `--worker_slots` - Run worker node with concurrent jobs, you must have some nodes with worker_slots to use remote execution, default: `0`
- `--download_concurrency` - how many concurrent file downloads, default: `10`
- `--compress` - compress all blobs before storage, default: `true`

### Debugging
- `--no_cleanup_execroot` - Don't delete execroot on `fail` or `all`, default: none
- `--loglevel` - logrus log level, default: `info`

