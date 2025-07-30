package service

import (
	"KVDB/internal/domain"
	"log"
)

type UpdateInstancesService struct {
	manager *domain.DbInstanceManager
}

func NewUpdateInstancesService(manager *domain.DbInstanceManager) *UpdateInstancesService {
	return &UpdateInstancesService{
		manager: manager,
	}
}

func (u UpdateInstancesService) Execute(instances []domain.DbInstance) {
	u.manager.SetReplicas(&instances)
	log.Println("Updated instance replicas, total replicas:", len(instances))
}
