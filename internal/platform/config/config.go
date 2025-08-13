package config

import (
	"flag"
	"github.com/joho/godotenv"
	"os"
)

const (
	ReliableBroadcastAlgorithm = "rb"
	EventualAlgorithm          = "ev"
)

var portCmd = flag.Int("port", 3000, "HTTP server port")
var algorithmCmd = flag.String("algorithm", "ev", "Algorithm used to maintain consistency between replicas. Options: 'ev', 'rb'.")

type Config struct {
	ServerPort      int
	WalDirectory    string
	ConfigServerUrl string
	DeploymentMode  string
	Algorithm       string
}

func LoadConfig() Config {
	godotenv.Load(".env")
	return Config{
		ServerPort:      *portCmd,
		WalDirectory:    os.Getenv("WAL_DIRECTORY"),
		ConfigServerUrl: os.Getenv("CONFIG_SERVER_URL"),
		DeploymentMode:  os.Getenv("DEPLOYMENT_MODE"),
		Algorithm:       *algorithmCmd,
	}
}
