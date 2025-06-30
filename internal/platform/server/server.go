package server

import (
	"KVDB/internal/platform/server/handler/dbentry"
	"KVDB/internal/platform/server/handler/health"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"log"
	"net/http"
)

type Server struct {
	httpAddr string
	engine   *chi.Mux
}

func NewServer(host string, port int) Server {
	url := fmt.Sprintf("%s:%d", host, port)
	srv := Server{
		engine:   chi.NewRouter(),
		httpAddr: url,
	}
	srv.engine.Use(middleware.Logger)
	srv.registerRoutes()
	return srv
}

func (s *Server) Run() error {
	log.Println("Server Running on:", s.httpAddr)
	return http.ListenAndServe(s.httpAddr, s.engine)
}

func (s *Server) registerRoutes() {
	s.engine.Get("/health", health.CheckHandler)
	s.engine.Get("/db/{key}", dbentry.GetEntry)
	s.engine.Post("/db/{key}", dbentry.SaveEntry)
	s.engine.Delete("/db/{key}", dbentry.DeleteEntry)
}
