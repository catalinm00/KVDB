package bootstrap

import (
	"KVDB/internal/application/service"
	"KVDB/internal/domain"
	"KVDB/internal/platform/client"
	"KVDB/internal/platform/config"
	"KVDB/internal/platform/repository"
	"KVDB/internal/platform/repository/lsm_tree"
	"KVDB/internal/platform/server"
	"KVDB/internal/platform/server/handler/dbentry"
	"KVDB/internal/platform/server/handler/dbinstance"
	"go.uber.org/dig"
	"log"
)

func Run() (bool, error) {
	container := dig.New()
	serviceConstructors := []interface{}{
		wal,
		domain.NewDbInstanceManager,
		lsm_tree.NewMemtable,
		repository.NewLSMTreeRepository,
		service.NewDeleteEntryService,
		service.NewSaveEntryService,
		service.NewGetEntryService,
		dbentry.NewDbEntryHandler,
		service.NewInstanceAutoRegisterService,
		service.NewUpdateInstancesService,
		server.NewServer,
		config.LoadConfig,
		dbinstance.NewDbInstanceHandler,
		configServerClient,
	}
	for _, service := range serviceConstructors {
		if err := container.Provide(service); err != nil {
			return false, err
		}
	}
	err := container.Invoke(func(s server.Server, ar *service.InstanceAutoRegisterService) {
		ar.Execute()
		s.Run()
		log.Println("Dependencies OK")
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func wal() (*lsm_tree.WAL, error) {
	dir := config.LoadConfig().WalDirectory
	return lsm_tree.NewWal(dir)
}

func configServerClient() *client.ConfigServerClient {
	url := config.LoadConfig().ConfigServerUrl
	return client.NewConfigServerClient(url)
}
