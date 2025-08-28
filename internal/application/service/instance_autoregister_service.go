package service

import (
	"KVDB/internal/domain"
	"KVDB/internal/domain/strategy"
	"KVDB/internal/platform/client"
	"KVDB/internal/platform/config"
	"log"
	"net"
	"strings"
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
	if strings.Contains(i.config.DeploymentMode, "devel") {
		return "localhost"
	}
	ips, err := GetLocalIPs()
	if err != nil {
		return "localhost"
	}
	log.Println(ips)
	return ips[0].String()
}

func GetLocalIPs() ([]net.IP, error) {
	var ips []net.IP
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, addr := range addresses {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP)
			}
		}
	}
	return ips, nil
}
