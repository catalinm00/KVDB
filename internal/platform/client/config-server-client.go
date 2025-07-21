package client

import (
	"KVDB/internal/domain"
	"github.com/go-resty/resty/v2"
)

const (
	instances_endpoint = "/api/v1/instances"
)

type ConfigServerClient struct {
	client    *resty.Client
	serverUrl string
}

func NewConfigServerClient(configServerUrl string) *ConfigServerClient {
	return &ConfigServerClient{
		client:    resty.New(),
		serverUrl: configServerUrl,
	}
}

func (c *ConfigServerClient) RegisterInstance(inst domain.DbInstance) (*domain.DbInstance, error) {
	var resp domain.DbInstance
	uri := c.serverUrl + instances_endpoint
	body := RegisterInstanceRequest{
		Host: inst.Host,
		Port: inst.Port,
	}
	_, err := c.client.R().SetResult(&resp).SetBody(&body).Post(uri)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *ConfigServerClient) FindAllInstances() (*[]domain.DbInstance, error) {
	var resp []domain.DbInstance
	uri := c.serverUrl + instances_endpoint

	_, err := c.client.R().SetResult(&resp).Get(uri)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
