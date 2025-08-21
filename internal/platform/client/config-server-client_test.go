package client

import (
	"KVDB/internal/domain"
	json "github.com/json-iterator/go"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper para crear instancia mock
func mockInstance() domain.DbInstance {
	return domain.DbInstance{
		Id:   1,
		Host: "localhost",
		Port: 5432,
	}
}

func TestRegisterInstance(t *testing.T) {
	instance := mockInstance()

	// Crear servidor HTTP de prueba
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/instances", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req RegisterInstanceRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		assert.NoError(t, err)

		resp := domain.DbInstance{
			Id:   1,
			Host: req.Host,
			Port: req.Port,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cli := NewConfigServerClient(server.URL)
	resp, err := cli.RegisterInstance(instance)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, instance.Host, resp.Host)
	assert.Equal(t, instance.Port, resp.Port)
	assert.Equal(t, uint64(1), resp.Id)
}

func TestFindAllInstances(t *testing.T) {
	expected := []domain.DbInstance{mockInstance()}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/instances", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expected)
	}))
	defer server.Close()

	cli := NewConfigServerClient(server.URL)
	result, err := cli.FindAllInstances()

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, *result, len(expected))
	assert.Equal(t, expected[0], (*result)[0])
}
