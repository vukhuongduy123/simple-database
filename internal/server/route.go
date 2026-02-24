package server

import (
	"context"
	"encoding/json"
	"net/http"
	"simple-database/internal/commandhandler"
	"simple-database/internal/platform/helper"
	"sync"
	"time"
)

var (
	mux  *http.ServeMux
	once sync.Once
)

type QueryRequest struct {
	Sql string `json:"query"`
}

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	helper.Log.Debugf("Handler query %s", r.URL.Path)
	start := time.Now()
	// Always set response headers
	w.Header().Set("Content-Type", "application/json")

	// Add request timeout
	_, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPost:
		handleQuery(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	helper.Log.Debugf("%s %s completed in %v", r.Method, r.URL.Path, time.Since(start))
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	var input QueryRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	handler, err := commandhandler.GetSqlCommandHandler()
	if err != nil {
		helper.Log.Errorf("Error getting sql command handler: %s", err.Error())
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	result, err := handler.Execute(input.Sql)
	if err != nil {
		helper.Log.Errorf("Error executing sql command handler: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

func GetServer() *http.ServeMux {
	once.Do(func() {
		mux = http.NewServeMux()
		handler := http.HandlerFunc(QueryHandler)
		mux.HandleFunc("/request", handler)
	})
	return mux
}
