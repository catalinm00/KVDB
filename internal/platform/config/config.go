package config

import (
	"flag"
	"github.com/joho/godotenv"
	"os"
)

const (
	ReliableBroadcastAlgorithm = "rb"
	EventualAlgorithm          = "ev"
	AtomicBoAlgorithm          = "at"
)

var portCmd = flag.Int("port", 3000, "HTTP server port")
var algorithmCmd = flag.String("algorithm", "rb", "Algorithm used to maintain consistency between replicas. Options: 'ev', 'rb', 'at'.")
var sequencerCmd = flag.String("sequencer-url", "localhost", "Specify url of the sequencer for atomic broadcast")

type Config struct {
	ServerPort        int
	ZmqApiPort        int
	WalDirectory      string
	ConfigServerUrl   string
	SequencerHost     string
	SequencerPullPort int
	SequencerPubPort  int
	DeploymentMode    string
	Algorithm         string
}

func LoadConfig() Config {
	godotenv.Load(".env")
	return Config{
		ServerPort:        *portCmd,
		ZmqApiPort:        *portCmd + 7,
		SequencerHost:     *sequencerCmd,
		SequencerPubPort:  7000,
		SequencerPullPort: 7001,
		WalDirectory:      os.Getenv("WAL_DIRECTORY"),
		ConfigServerUrl:   os.Getenv("CONFIG_SERVER_URL"),
		DeploymentMode:    os.Getenv("DEPLOYMENT_MODE"),
		Algorithm:         *algorithmCmd,
	}
}
