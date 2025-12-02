package main

import (
	"fmt"
	"log"
	"os"
	"simple-database/internal"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/helper"
	"simple-database/internal/table"
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
	id, err := column.NewColumn("id", datatype.TypeInt64, column.PrimaryKey)
	if err != nil {
		log.Fatal(err)
	}
	username, err := column.NewColumn("username", datatype.TypeString, column.UsingIndex)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.CreateTable(
		"users",
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

	{
		start := time.Now()
		for i := 0; i < 2000_000; i++ {
			helper.Log.Debugf("Inserting user %d", i)
			_, err = db.Tables["users"].Insert(
				map[string]interface{}{
					"id":       int64(i),
					"username": "This is a user " + fmt.Sprint(i),
				},
			)
			if err != nil {
				fmt.Println(err)
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("Time elapsed insert: %s\n", elapsed)
	}

	{
		start := time.Now()
		newValueMap := map[string]any{}
		for i := 0; i < 2000_000; i++ {
			helper.Log.Debugf("Updating user %d", i)
			whereClause := make(map[string]table.Comparator)
			whereClause["id"] = table.Comparator{
				Operator: datatype.OperatorEqual,
				Value:    int64(i),
			}

			newValueMap["username"] = "This is a user " + fmt.Sprint(-i)
			_, err = db.Tables["users"].Update(
				table.SelectCommand{
					WhereClause: whereClause,
					Limit:       1,
				}, newValueMap)
			if err != nil {
				log.Fatal(err)
			}
		}

		elapsed := time.Since(start)
		fmt.Printf("Time elapsed update: %s\n", elapsed)
	}

	{
		start := time.Now()
		resultSet, e := db.Tables["users"].Select(table.SelectCommand{
			Limit: 1000000,
			WhereClause: map[string]table.Comparator{
				"username": {
					Operator: datatype.OperatorEqual,
					Value:    "This is a user -159",
				},
			},
		})
		if e != nil {
			log.Fatal(e)
		}

		elapsed := time.Since(start)
		fmt.Printf("Time elapsed selecting after update: %s for %d\n", elapsed, len(resultSet.Rows))
		for idx, result := range resultSet.Rows {
			fmt.Printf("%d: %v\n", idx, result)
		}
	}

	defer func(db *internal.Database) {
		err := db.Close()
		if err != nil {

		}
	}(db)
}
