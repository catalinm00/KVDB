package dbinstance

import "KVDB/internal/domain"

type SaveDbInstancesRequest struct {
	Instances []domain.DbInstance `json:"Instances,omitempty"`
}
