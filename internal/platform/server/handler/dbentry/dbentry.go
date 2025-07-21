package dbentry

import (
	"KVDB/internal/application/service"
	"KVDB/internal/domain"
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"io/ioutil"
	"net/http"
)

type DbEntryHandler struct {
	saveService   *service.SaveEntryService
	deleteService *service.DeleteEntryService
	getService    *service.GetEntryService
}

type EntryResponse struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Tombstone bool   `json:"tombstone"`
}

func MapToEntryResponse(e domain.DbEntry) EntryResponse {
	return EntryResponse{
		Key:       e.Key(),
		Value:     e.Value(),
		Tombstone: e.Tombstone(),
	}
}

func NewDbEntryHandler(saveService *service.SaveEntryService,
	deleteService *service.DeleteEntryService,
	getService *service.GetEntryService) *DbEntryHandler {
	return &DbEntryHandler{
		saveService:   saveService,
		deleteService: deleteService,
		getService:    getService,
	}
}

func (h *DbEntryHandler) SaveEntry(w http.ResponseWriter, r *http.Request) {
	var request SaveEntryRequest
	body, err := ioutil.ReadAll(r.Body)
	err = json.Unmarshal([]byte(body), &request)
	if err != nil {
		fmt.Fprintf(w, err.Error())
	}
	result := h.saveService.Execute(service.SaveEntryCommand{
		Key:   request.Key,
		Value: request.Value,
	})
	output, _ := json.Marshal(MapToEntryResponse(result.Entry))
	fmt.Fprintf(w, string(output))
}

func (h *DbEntryHandler) GetEntry(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	result := h.getService.Execute(service.GetEntryQuery{
		Key: key,
	})
	if !result.Found {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Not found")
		return
	}
	output, _ := json.Marshal(MapToEntryResponse(result.Entry))
	fmt.Fprintf(w, string(output))
}

func (h *DbEntryHandler) DeleteEntry(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	result := h.deleteService.Execute(service.DeleteEntryCommand{
		Key: key,
	})
	output, _ := json.Marshal(MapToEntryResponse(result.Entry))
	fmt.Fprintf(w, string(output))
}
