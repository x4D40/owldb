package main

import (
	"flag"
	"os"
	"os/signal"
	"owldb/database"
	"strconv"
	"strings"
	"syscall"

	"github.com/google/uuid"
)

func main() {

	config := parseConfig()
	db := database.NewDatabase(config)

	if err := db.Join(); err != nil {
		panic(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	if err := db.Leave(); err != nil {
		panic(err)
	}
}

func getEnvString(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}

	i, err := strconv.Atoi(value)

	if err != nil {
		panic(err.Error())
	}

	return i
}

func parseConfig() database.Config {
	cfg := database.Config{}

	discoverEndpoints := ""

	flag.StringVar(&cfg.Id, "node", getEnvString("ODB_NODE", uuid.NewString()), "The id of this node, defaults to a uuid")
	flag.StringVar(&cfg.Host, "host", getEnvString("ODB_HOST", "127.0.0.1"), "The hostname for this node, defaults to localhost")
	flag.IntVar(&cfg.GossipPort, "sport", getEnvInt("ODB_SPORT", 4008), "This sync port used for this node, defaults to 4008")
	flag.IntVar(&cfg.Port, "port", getEnvInt("ODB_PORT", 4007), "The port clients connect to for database operations, defaults to 4007")
	flag.StringVar(&discoverEndpoints, "join", getEnvString("ODB_JOIN", ""), "The endpoints to attempt to join this node too")

	flag.Parse()

	if discoverEndpoints != "" {
		cfg.Join = strings.Split(discoverEndpoints, ";")
	}

	return cfg
}
