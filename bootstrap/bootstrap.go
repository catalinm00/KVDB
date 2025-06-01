package bootstrap

import (
	"KVDB/internal/platform/server"
)

const (
	host = "localhost"
	port = 3000
)

func Run() error {
	srv := server.New(host, port)
	return srv.Run()
}
