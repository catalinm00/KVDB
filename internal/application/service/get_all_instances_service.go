package service

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/client"
	"log"
)

type GetAllInstancesService struct {
	configServer    *client.ConfigServerClient
	instanceManager *domain.DbInstanceManager
}

func NewGetAllInstancesService(configServer *client.ConfigServerClient,
	instanceManager *domain.DbInstanceManager) *GetAllInstancesService {
	return &GetAllInstancesService{
		configServer:    configServer,
		instanceManager: instanceManager,
	}
}

func (g *GetAllInstancesService) Execute() error {
	instances, err := g.configServer.FindAllInstances()
	if err != nil {
		return err
	}

	g.instanceManager.SetReplicas(instances)
	log.Println("Retrieved", len(*instances), "replica instances from config server")
	return nil
}
