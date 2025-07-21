package client

type RegisterInstanceRequest struct {
	Host string `json:"host,omitempty"`
	Port int    `json:"port,omitempty"`
}
