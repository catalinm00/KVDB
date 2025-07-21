package config

import "os"

type Config struct {
	ServerPort      string
	WalDirectory    string
	ConfigServerUrl string
}

func LoadConfig() Config {
	return Config{
		ServerPort:      os.Getenv("HTTP_SERVER_PORT"),
		WalDirectory:    os.Getenv("WAL_DIRECTORY"),
		ConfigServerUrl: os.Getenv("CONFIG_SERVER_URL"),
	}
}
