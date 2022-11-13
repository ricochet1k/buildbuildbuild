package server

type Config struct {
	Listen string
	Port   int

	NodeName    string
	ClusterHost string
	ClusterPort int
	Autojoin    string
	Join        string

	Bucket              string
	Region              string
	WorkerSlots         int
	DownloadConcurrency int
	CacheDir            string
}
