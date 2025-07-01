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

const (
	host = "localhost"
	port = 3000
)

type Server struct {
	httpAddr     string
	engine       *chi.Mux
	entryHandler *dbentry.DbEntryHandler
}

func NewServer(entryHandler *dbentry.DbEntryHandler) Server {
	url := fmt.Sprintf("%s:%d", host, port)
	srv := Server{
		engine:       chi.NewRouter(),
		httpAddr:     url,
		entryHandler: entryHandler,
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
	s.engine.Route("/api", func(r chi.Router) {
		r.Get("/db/{key}", s.entryHandler.GetEntry)
		r.Post("/db", s.entryHandler.SaveEntry)
		r.Delete("/db/{key}", s.entryHandler.DeleteEntry)
	})
}
