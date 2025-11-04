package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"simple-database/internal"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/table/column"
	"time"
)

func main() {
	_ = os.RemoveAll("data")
	_ = os.Remove("cpu.prof")

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

	start := time.Now()
	for i := 0; i < 1000; i++ {
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
		fmt.Printf("Time elapsed %d: %s\n", i, elapsed)
	}
	elapsed := time.Since(start)
	fmt.Printf("Time elapsed running 10M insert: %s\n", elapsed)

	start = time.Now()
	resultSet, err := db.Tables["users"].Select(map[string]interface{}{
		"id": int64(500),
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)

	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	fmt.Println(resultSet)

	start = time.Now()
	resultSet, err = db.Tables["users"].Select(map[string]interface{}{
		"username": "This is a user 500",
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)
	fmt.Println(resultSet)

	{
		start = time.Now()
		resultSet, err = db.Tables["users"].Select(nil)
		elapsed = time.Since(start)
		fmt.Printf("Time elapsed: %s\n", elapsed)
		results := resultSet.Rows
		for idx, result := range results {
			fmt.Printf("%d: %v\n", idx, result)
		}
	}

	{
		start = time.Now()
		oldValueMap := map[string]any{}
		newValueMap := map[string]any{}
		for i := 0; i < 1000; i++ {
			fmt.Println("Updating index " + fmt.Sprint(i))

			oldValueMap["id"] = int64(i)
			newValueMap["username"] = "This is a user " + fmt.Sprint(-i)
			_, err = db.Tables["users"].Update(oldValueMap, newValueMap)
			if err != nil {
				log.Fatal(err)
			}
		}

		elapsed = time.Since(start)
		fmt.Printf("Time elapsed: %s\n", elapsed)
	}

	{
		start = time.Now()
		resultSet, err = db.Tables["users"].Select(nil)
		elapsed = time.Since(start)
		fmt.Printf("Time elapsed: %s\n", elapsed)
		results := resultSet.Rows
		for idx, result := range results {
			fmt.Printf("%d: %v\n", idx, result)
		}
	}

	defer func(db *internal.Database) {
		err := db.Close()
		if err != nil {

		}
	}(db)
}
