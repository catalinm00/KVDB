package config

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"strconv"
)

type Config struct {
	ServerPort      int
	WalDirectory    string
	ConfigServerUrl string
	DeploymentMode  string
}

func LoadConfig() Config {
	godotenv.Load(".env")
	port, err := strconv.Atoi(os.Getenv("HTTP_SERVER_PORT"))
	if err != nil {
		fmt.Println("Error:", err)
	}
	return Config{
		ServerPort:      port,
		WalDirectory:    os.Getenv("WAL_DIRECTORY"),
		ConfigServerUrl: os.Getenv("CONFIG_SERVER_URL"),
		DeploymentMode:  os.Getenv("DEPLOYMENT_MODE"),
	}
}
