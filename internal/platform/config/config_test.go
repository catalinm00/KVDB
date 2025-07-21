package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Arrange
	os.Setenv("HTTP_SERVER_PORT", "8080")
	os.Setenv("WAL_DIRECTORY", "/var/logs/wal")
	os.Setenv("CONFIG_SERVER_URL", "http://config-service.local")

	// Act
	cfg := LoadConfig()

	// Assert
	if cfg.ServerPort != "8080" {
		t.Errorf("expected ServerPort '8080', got '%s'", cfg.ServerPort)
	}
	if cfg.WalDirectory != "/var/logs/wal" {
		t.Errorf("expected WalDirectory '/var/logs/wal', got '%s'", cfg.WalDirectory)
	}
	if cfg.ConfigServerUrl != "http://config-service.local" {
		t.Errorf("expected ConfigServerUrl 'http://config-service.local', got '%s'", cfg.ConfigServerUrl)
	}
}
