package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	p "github.com/cswank/parquet"
)

var (
	j = flag.Bool("json", false, "write json instead of parquet")
)

func main() {
	flag.Parse()

	if *j {
		writeJSON()
	} else {
		writeParq()
	}
}

func writeParq() {
	f, err := os.Open("records.json")
	if err != nil {
		log.Fatal(err)
	}

	var records []p.Record
	err = json.NewDecoder(f).Decode(&records)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()

	f, err = os.Create("records.parquet")
	if err != nil {
		log.Fatal(err)
	}

	rec := p.New(f, p.MaxPageSize(10))

	for _, r := range records {
		rec.Add(r)
	}

	if err := rec.Write(); err != nil {
		log.Fatal(err)
	}

	f.Close()
}

func writeJSON() {
	records := []p.Record{}
	for i := 0; i < 2000; i++ {
		var age *int32
		if i%2 == 0 {
			a := int32(20 + i%5)
			age = &a
		}
		records = append(records, p.Record{
			ID:  int32(i),
			Age: age,
		})
	}

	f, err := os.Create("records.json")
	if err != nil {
		log.Fatal(err)
	}

	err = json.NewEncoder(f).Encode(records)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()
}
