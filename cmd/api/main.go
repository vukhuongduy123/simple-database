package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"simple-database/internal/table/btree"
)

func profiling() {
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
}

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

	/*for i := 0; i < 2000_000; i++ {
		if i%1000 == 0 {
			fmt.Println(i)
		}
		err = b.Remove([]byte(fmt.Sprint(i)))
		if err != nil {
			log.Fatal(err)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(-i))
		err = b.Insert(buf, buf)
		if err != nil {
			log.Fatal(err)
		}
	}
	size, err = b.Size()
	if err != nil {
		log.Fatal(err)
	}*/
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
	// profiling()
	testBree()

	/*db, err := internal.CreateDatabase("my_db")
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
				panic(err)
			}
		}
		elapsed := time.Since(start)
		helper.Log.Debugf("Time elapsed insert: %s. Insertion speed %f/seconds\n", elapsed, 2000_000/elapsed.Seconds())
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
					Limit:       table.UnlimitedSize,
				}, newValueMap)
			if err != nil {
				log.Fatal(err)
			}
		}

		elapsed := time.Since(start)
		helper.Log.Debugf("Time elapsed update: %s.Update speed %f/seconds\n", elapsed, 2000_000/elapsed.Seconds())
	}

	{
		start := time.Now()
		resultSet, e := db.Tables["users"].Select(table.SelectCommand{
			Limit: table.UnlimitedSize,
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

	{
		for i := 0; i < 2000_000; i++ {
			start := time.Now()
			resultSet, e := db.Tables["users"].Select(table.SelectCommand{
				Limit: 1000000,
				WhereClause: map[string]table.Comparator{
					"id": {
						Operator: datatype.OperatorEqual,
						Value:    int64(i),
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
	}

	defer func(db *internal.Database) {
		err := db.Close()
		if err != nil {
		}
	}(db)*/
}
