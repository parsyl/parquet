package main

//go:generate parquetgen --parquet ./people.parquet --type Person --package main

import (
	"encoding/json"
	"log"
	"os"
)

func main() {
	f, err := os.Open("people.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r, err := NewParquetReader(f)
	if err != nil {
		log.Fatal(err)
	}

	enc := json.NewEncoder(os.Stdout)
	for r.Next() {
		var p Person
		r.Scan(&p)
		enc.Encode(p)
	}
}
