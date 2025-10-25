package main

import (
	"log"
	"simple-database/internal"
	"simple-database/internal/platform/parser"
	"simple-database/internal/table/column"
)

func main() {
	db, err := internal.CreateDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}
	id, err := column.NewColumn("id", parser.TypeInt64, column.NewOpts(false))
	if err != nil {
		log.Fatal(err)
	}
	username, err := column.NewColumn("username", parser.TypeString, column.NewOpts(false))
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.CreateTable(
		"users",
		[]string{"id", "username"},
		map[string]*column.Column{
			"id":       id,
			"username": username,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

}
