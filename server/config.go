package server

type Config struct {
	Listen string
	Port   int

	ClusterName string
	ClusterHost string
	ClusterPort int
	Join        string

	Bucket string
	Region string
	Worker bool
}
