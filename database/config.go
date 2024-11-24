package database

type Config struct {
	Id         string
	Host       string
	GossipPort int
	Port       int
	Leader     bool
	Join       []string
}
