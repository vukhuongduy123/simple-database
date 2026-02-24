package main

import (
	"net/http"
	"simple-database/internal/platform/helper"
	"simple-database/internal/server"
)

func main() {
	helper.Log.Infof("Server started on port 8080")
	err := http.ListenAndServe(":8080", server.GetServer())
	if err != nil {
		return
	}
}
