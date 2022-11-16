package server

type Config struct {
	// GRPC
	Listen string
	Port   int

	// Serf Cluster
	NodeName      string
	BindHost      string
	BindPort      int
	AdvertiseHost string
	AdvertisePort int
	Autojoin      string
	Join          string

	Bucket              string
	Region              string
	WorkerSlots         int
	DownloadConcurrency int
	CacheDir            string

	NoCleanupExecroot string
}
