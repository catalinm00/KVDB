package config

import (
	"flag"
	"github.com/joho/godotenv"
	"os"
)

var portCmd = flag.Int("port", 3000, "HTTP server port")

type Config struct {
	ServerPort      int
	WalDirectory    string
	ConfigServerUrl string
	DeploymentMode  string
}

func LoadConfig() Config {
	godotenv.Load(".env")
	return Config{
		ServerPort:      *portCmd,
		WalDirectory:    os.Getenv("WAL_DIRECTORY"),
		ConfigServerUrl: os.Getenv("CONFIG_SERVER_URL"),
		DeploymentMode:  os.Getenv("DEPLOYMENT_MODE"),
	}
}
