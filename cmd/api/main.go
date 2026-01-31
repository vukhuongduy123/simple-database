package main

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"simple-database/internal/commandhandler"
	"simple-database/internal/engine/table"
	"simple-database/internal/platform/helper"
	"time"
)

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

	handler, err := commandhandler.GetSqlCommandHandler()
	if err != nil {
		log.Fatal(err)
	}
	_, err = handler.Execute("CREATE TABLE users (id INT64 PRIMARY KEY, username STRING, age INT32 INDEX, record INT32)")
	if err != nil {
		log.Fatal(err)
	}

	iterator := 1000

	{
		start := time.Now()
		for i := 0; i < iterator; i++ {
			sql := fmt.Sprintf("INSERT INTO users (id, username, age, record) VALUES (INT64(%d), STRING('This is a user %d'), INT32(%d), INT32(%d))",
				i, i, i%10_000, i%10_000)
			_, err = handler.Execute(sql)
			if err != nil {
				log.Fatal(err)
			}
		}
		elapsed := time.Since(start)
		helper.Log.Debugf("Time elapsed insert: %s. Insertion speed %f/seconds\n", elapsed, float64(iterator)/elapsed.Seconds())
	}

	{
		for i := 0; i < 1; i++ {
			fmt.Printf("Select age\n")
			start := time.Now()
			sql := fmt.Sprintf("SELECT * FROM users WHERE age <= INT32(129) LIMIT 10000000")
			resultSet, e := handler.Execute(sql)
			if e != nil {
				log.Fatal(e)
			}

			elapsed := time.Since(start)
			fmt.Printf("Select age value %d: %s for %v\n", i, elapsed, resultSet.(*table.SelectResult))
			for idx, result := range resultSet.(*table.SelectResult).Rows {
				fmt.Printf("%d: %v\n", idx, result)
			}
		}
	}

	{
		for i := 0; i < 1; i++ {
			fmt.Printf("Select record\n")
			start := time.Now()
			sql := fmt.Sprintf("SELECT * FROM users WHERE record <= INT32(129) LIMIT 10000000")
			resultSet, e := handler.Execute(sql)
			if e != nil {
				log.Fatal(e)
			}

			elapsed := time.Since(start)
			fmt.Printf("Select age value %d: %s for %v\n", i, elapsed, resultSet.(*table.SelectResult))
			for idx, result := range resultSet.(*table.SelectResult).Rows {
				fmt.Printf("%d: %v\n", idx, result)
			}
		}
	}
}
