package main

import (
	"fmt"
	"log"
	"os"
	"simple-database/internal"
	"simple-database/internal/platform/datatype"
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
	fmt.Printf("Time elapsed running 10M insert: %s\n", elapsed)

	start = time.Now()
	whereClause := make(map[string]table.Comparator)
	whereClause["id"] = table.Comparator{
		Operator: datatype.OperatorEqual,
		Value:    int64(500),
	}

	resultSet, err := db.Tables["users"].Select(table.SelectCommand{
		WhereClause: whereClause,
		Limit:       10,
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed selecting with index: %s\n", elapsed)
	fmt.Println(resultSet)

	start = time.Now()
	whereClause = make(map[string]table.Comparator)
	whereClause["id"] = table.Comparator{
		Operator: datatype.OperatorEqual,
		Value:    int64(501),
	}
	resultSet, err = db.Tables["users"].Select(table.SelectCommand{
		WhereClause: whereClause,
		Limit:       10,
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed selecting with cache: %s\n", elapsed)
	fmt.Println(resultSet)

	whereClause = make(map[string]table.Comparator)
	whereClause["username"] = table.Comparator{
		Operator: datatype.OperatorEqual,
		Value:    "This is a user 500",
	}
	resultSet, err = db.Tables["users"].Select(table.SelectCommand{
		WhereClause: whereClause,
		Limit:       10,
	})
	elapsed = time.Since(start)
	fmt.Printf("Time elapsed selecting no index: %s\n", elapsed)
	fmt.Println(resultSet)

	{
		start = time.Now()
		resultSet, err = db.Tables["users"].Select(table.SelectCommand{})
		elapsed = time.Since(start)
		fmt.Printf("Time elapsed selecting all: %s\n", elapsed)
		results := resultSet.Rows
		for idx, result := range results {
			fmt.Printf("%d: %v\n", idx, result)
		}
	}

	{
		start = time.Now()
		newValueMap := map[string]any{}
		for i := 0; i < 1000; i++ {
			if i == 696 {
				fmt.Printf("Found it\n")
			}
			whereClause = make(map[string]table.Comparator)
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

		elapsed = time.Since(start)
		fmt.Printf("Time elapsed update: %s\n", elapsed)
	}

	{
		start = time.Now()
		resultSet, err = db.Tables["users"].Select(table.SelectCommand{})
		elapsed = time.Since(start)
		fmt.Printf("Time elapsed selecting after update: %s for %d\n", elapsed, len(resultSet.Rows))
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
