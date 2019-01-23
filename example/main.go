package main

import (
	"log"
	"os"

	p "github.com/cswank/parquet"
)

func main() {
	records := []p.Record{}
	for i := 0; i < 2; i++ {
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

	f, err := os.Create("rec.parquet")
	if err != nil {
		log.Fatal(err)
	}

	rec := p.New(f)

	for _, r := range records {
		rec.Add(r)
	}

	if err := rec.Write(); err != nil {
		log.Fatal(err)
	}

	f.Close()
}
