package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"simple-database/internal"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/evaluator"
	"simple-database/internal/platform/helper"
	"simple-database/internal/table"
	"simple-database/internal/table/btree"
	"simple-database/internal/table/column"
	"time"
)

func testBree() {
	_ = os.MkdirAll("data/test", 0777)
	b, err := btree.Open("data/test/mydb")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		if i%1000 == 0 {
			fmt.Println(i)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		err := b.Insert(buf, buf)
		if err != nil {
			log.Fatal(err)
		}
	}
	_ = b.PrintTree()

	_ = b.Remove([]byte(fmt.Sprint(0)))
	_ = b.PrintTree()
	size, err := b.Size()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Size of btree", size)

	fmt.Println("Size of btree", size)
	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		keys, err := b.LessThanOrEqual(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Size of less than or equal to ", i, " is ", len(keys))
	}

	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		keys, err := b.LessThan(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Size of less than ", i, " is ", len(keys))
	}

	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		keys, err := b.GreaterThanOrEqual(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Size of greater than or equal to ", i, " is ", len(keys))
	}

	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		keys, err := b.GreaterThan(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Size of greater than ", i, " is ", len(keys))
	}

	{
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(255))
		key, _, err := b.Get(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Get ", 255, " is ", key)
	}

	for i := 0; i < 1000; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		key, _, err := b.Get(buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Get ", i, " is ", key)
	}

	defer func(b *btree.BTree) {
		err := b.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(b)

}

func main() {
	_ = os.RemoveAll("data")
	_ = os.Remove("cpu.prof")
	_ = os.Remove("mem.prof")

	// At the start of main()
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer func(cpuProfile *os.File) {
		err := cpuProfile.Close()
		if err != nil {
		}
	}(cpuProfile)
	if err := pprof.StartCPUProfile(cpuProfile); err != nil {
		log.Fatal("could not start CPU prof: ", err)
	}
	defer pprof.StopCPUProfile()

	// At the end of main(), before exiting
	memProfile, err := os.Create("mem.prof")
	if err != nil {
		log.Fatal("could not create memory prof: ", err)
	}
	defer func(memProfile *os.File) {
		err := memProfile.Close()
		if err != nil {
		}
	}(memProfile)
	_ = pprof.WriteHeapProfile(memProfile)

	//testBree()

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
	age, err := column.NewColumn("age", datatype.TypeInt32, column.UsingIndex)
	if err != nil {
		log.Fatal(err)
	}
	record, err := column.NewColumn("record", datatype.TypeInt32, column.Normal)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.CreateTable(
		"users",
		map[string]*column.Column{
			"id":       id,
			"username": username,
			"age":      age,
			"record":   record,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	db, err = internal.NewDatabase("my_db")
	if err != nil {
		log.Fatal(err)
	}

	iterator := 1000
	{
		start := time.Now()
		for i := 0; i < iterator; i++ {
			//helper.Log.Debugf("Inserting user %d", i)
			_, err = db.Tables["users"].Insert(
				map[string]interface{}{
					"id":       int64(i),
					"username": "This is a user " + fmt.Sprint(i),
					"age":      int32(i % 10_000),
					"record":   int32(i % 10_000),
				},
			)
			if err != nil {
				panic(err)
			}
		}
		elapsed := time.Since(start)
		helper.Log.Debugf("Time elapsed insert: %s. Insertion speed %f/seconds\n", elapsed, float64(iterator)/elapsed.Seconds())
	}

	/*{
		start := time.Now()
		newValueMap := map[string]any{}
		for i := 0; i < iterator; i++ {
			//helper.Log.Debugf("Updating user %d", i)
			e := &evaluator.Expression{
				Left:  "id",
				Op:    datatype.OperatorEqual,
				Right: int64(i),
			}

			newValueMap["username"] = "This is a user " + fmt.Sprint(-i)
			_, err = db.Tables["users"].Update(
				table.SelectCommand{
					Expression: e,
					Limit:      table.UnlimitedSize,
				}, newValueMap)
			if err != nil {
				log.Fatal(err)
			}
		}

		elapsed := time.Since(start)
		helper.Log.Debugf("Time elapsed update: %s.Update speed %f/seconds\n", elapsed, float64(iterator)/elapsed.Seconds())
	}

	{
		start := time.Now()
		resultSet, e := db.Tables["users"].Select(table.SelectCommand{
			Limit:       table.UnlimitedSize,
			WhereClause: nil,
		})
		if e != nil {
			log.Fatal(e)
		}

		elapsed := time.Since(start)
		fmt.Printf("Select all: %s for %v\n", elapsed, resultSet)
		for idx, result := range resultSet.Rows {
			fmt.Printf("%d: %v\n", idx, result)
		}
	}*/

	{
		for i := 0; i < 1; i++ {
			fmt.Printf("Select age\n")
			start := time.Now()
			resultSet, e := db.Tables["users"].Select(table.SelectCommand{
				Limit: table.UnlimitedSize,
				Expression: &evaluator.Expression{
					Left:  "age",
					Op:    datatype.OperatorLess,
					Right: int32(129),
				},
			})
			if e != nil {
				log.Fatal(e)
			}

			elapsed := time.Since(start)
			fmt.Printf("Select age value %d: %s for %v\n", i, elapsed, resultSet)
			for idx, result := range resultSet.Rows {
				fmt.Printf("%d: %v\n", idx, result)
			}
		}
	}

	{
		for i := 0; i < 1; i++ {
			fmt.Printf("Select record\n")
			start := time.Now()
			resultSet, e := db.Tables["users"].Select(table.SelectCommand{
				Limit: table.UnlimitedSize,
				Expression: &evaluator.Expression{
					Left:  "record",
					Op:    datatype.OperatorLessOrEqual,
					Right: int32(10),
				},
			})
			if e != nil {
				log.Fatal(e)
			}

			elapsed := time.Since(start)
			fmt.Printf("Select record value %d: %s for %v\n", i%10, elapsed, resultSet)
			/*for idx, result := range resultSet.Rows {
				fmt.Printf("%d: %v\n", idx, result)
			}*/
		}
	}

	defer func(db *internal.Database) {
		err := db.Close()
		if err != nil {
		}
	}(db)
}
