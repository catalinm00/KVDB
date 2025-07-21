package domain

type DbInstance struct {
	Id   uint64 `json:"id,omitempty"`
	Host string `json:"host,omitempty"`
	Port int    `json:"port,omitempty"`
}

type DbInstanceRepository interface {
	FindAll() []DbInstance
	SaveAll(instances *[]DbInstance) []DbInstance
}
