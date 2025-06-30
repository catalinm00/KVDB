package bootstrap

import (
	"KVDB/internal/platform/repository"
	"KVDB/internal/platform/repository/lsm_tree"
	"KVDB/internal/platform/server"
	"go.uber.org/dig"
	"log"
)

const (
	host = "localhost"
	port = 3000
)

func Run() error {
	srv := server.NewServer(host, port)
	container := dig.New()
	serviceConstructors := []interface{}{
		lsm_tree.NewSkipList,
		lsm_tree.NewWal,
		lsm_tree.NewMemtable,
		repository.NewLSMTreeRepository,
	}
	for _, service := range serviceConstructors {
		if err := container.Provide(service); err != nil {
			return err
		}
	}
	err := container.Invoke(func() {
		log.Println("Dependencies OK")
	})
	if err != nil {
		return err
	}
	return srv.Run()
}
