package main

import (
	"fmt"
	"io"
	"log"
	"simple-database/internal"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/table/column"
	"time"
)

func main() {
	db, err := internal.CreateDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}
	id, err := column.NewColumn("id", datatype.TypeInt64, column.NewOpts(false))
	if err != nil {
		log.Fatal(err)
	}
	username, err := column.NewColumn("username", datatype.TypeString, column.NewOpts(false))
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

	start := time.Now()
	for i := 0; i < 10_000_000; i++ {
		_, err = db.Tables["users"].Insert(
			map[string]interface{}{
				"id":       int64(i),
				"username": "This is a user",
			},
		)
		if err != nil {
			fmt.Println(err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)

	start = time.Now()
	resultSet, err := db.Tables["users"].Select(map[string]interface{}{
		"id": int64(1),
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)

	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	fmt.Println(resultSet)

}
