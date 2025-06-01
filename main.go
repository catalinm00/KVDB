package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/emirpasic/gods/trees/btree"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	fmt.Println("Starting app...")

	tree := btree.NewWithStringComparator(3)

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	// Ruta GET para obtener valor por clave
	r.Get("/db/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		resp, found := tree.Get(key)
		if found {
			fmt.Fprint(w, resp)
		} else {
			http.Error(w, "Key not found", http.StatusNotFound)
		}
	})

	// Ruta POST para insertar clave-valor
	r.Post("/db/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) == 0 {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		value := string(body)
		tree.Put(key, value)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "Inserted: %s => %s\n", key, value)
	})

	if err := http.ListenAndServe(":3000", r); err != nil {
		fmt.Println("Server error:", err)
	}
}
