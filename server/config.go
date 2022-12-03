package server

type Config struct {
	// GRPC
	Listen string
	Port   int

	// Serf Cluster
	ClusterName   string
	NodeName      string
	BindHost      string
	BindPort      int
	AdvertiseHost string
	AdvertisePort int
	AutojoinS3    string
	AutojoinUDP   string
	Join          string
	SecretKey     string

	RequiredHeader      string
	Bucket              string
	Region              string
	WorkerSlots         int
	DownloadConcurrency int
	CacheDir            string
	Compress            bool

	NoCleanupExecroot string
}
