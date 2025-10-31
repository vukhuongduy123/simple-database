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
	id, err := column.NewColumn("id", datatype.TypeInt64, true, column.NewOpts(false))
	if err != nil {
		log.Fatal(err)
	}
	username, err := column.NewColumn("username", datatype.TypeString, false, column.NewOpts(false))
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

	db, err = internal.NewDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100_000; i++ {
		start := time.Now()
		_, err = db.Tables["users"].Insert(
			map[string]interface{}{
				"id":       int64(i),
				"username": "This is a user " + fmt.Sprint(i),
			},
		)
		if err != nil {
			fmt.Println(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("Time elapsed: %s\n", elapsed)
	}

	start := time.Now()
	resultSet, err := db.Tables["users"].Select(map[string]interface{}{
		"id": int64(5),
	})
	elapsed := time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)

	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	fmt.Println(resultSet)

	start = time.Now()
	resultSet, err = db.Tables["users"].Select(map[string]interface{}{
		"username": "This is a user 5000",
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)
	fmt.Println(resultSet)

	defer func(db *internal.Database) {
		err := db.Close()
		if err != nil {

		}
	}(db)
}
