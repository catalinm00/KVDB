package service

import (
	"KVDB/internal/domain"
	"KVDB/internal/domain/strategy"
	"KVDB/internal/platform/client"
	"KVDB/internal/platform/config"
	"log"
	"net"
	"time"
)

type InstanceAutoRegisterService struct {
	configServer       *client.ConfigServerClient
	instanceManager    *domain.DbInstanceManager
	transactionManager *strategy.RbTransactionManager
	config             config.Config
}

func NewInstanceAutoRegisterService(configServer *client.ConfigServerClient, instanceManager *domain.DbInstanceManager,
	config config.Config) *InstanceAutoRegisterService {

	manager := InstanceAutoRegisterService{
		configServer:    configServer,
		instanceManager: instanceManager,
		config:          config,
	}
	return &manager
}

func (i *InstanceAutoRegisterService) Execute() {
	ip := i.getOutboundIP()
	instance := domain.DbInstance{
		Host: ip,
		Port: config.LoadConfig().ServerPort,
	}

	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	for {
		registeredInstance, err := i.configServer.RegisterInstance(instance)
		if err == nil {
			i.instanceManager.SetCurrentInstance(registeredInstance)
			log.Printf("Registered current instance with id %d\n", registeredInstance.Id)
			break
		}
		log.Printf("Failed to register instance: %v. Retrying in 60s...\n", err)
		<-ticker.C
	}
}

func (i *InstanceAutoRegisterService) getOutboundIP() string {
	if i.config.DeploymentMode == "devel" {
		return "localhost"
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}
