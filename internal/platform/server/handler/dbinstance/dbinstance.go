package dbinstance

import (
	"KVDB/internal/application/service"
	"KVDB/internal/domain"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type DbInstanceHandler struct {
	updateInstancesService *service.UpdateInstancesService
}

func NewDbInstanceHandler(updateInstancesService *service.UpdateInstancesService) *DbInstanceHandler {
	return &DbInstanceHandler{
		updateInstancesService: updateInstancesService,
	}
}

func (h *DbInstanceHandler) UpdateDbInstances(w http.ResponseWriter, r *http.Request) {
	var instances []domain.DbInstance
	body, err := ioutil.ReadAll(r.Body)
	err = json.Unmarshal([]byte(body), &instances)
	if err != nil {
		fmt.Fprintf(w, err.Error())
	}
	h.updateInstancesService.Execute(instances)
	w.WriteHeader(200)
	fmt.Fprintf(w, "Instances Updated Successfully")
}
