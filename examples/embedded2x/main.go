package main

//go:generate parquetgen -input main.go -type C -package main

import (
	"encoding/json"
	"log"
	"os"
)

func main() {
	f, err := os.Create("parquet")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	w, err := NewParquetWriter(f, MaxPageSize(2))
	if err != nil {
		log.Fatal(err)
	}

	w.Add(C{B: B{Name: "a", A: A{ID: 1}}})
	w.Add(C{B: B{Name: "b", A: A{ID: 2}}})
	w.Add(C{B: B{Name: "c", A: A{ID: 3}}})

	if err := w.Write(); err != nil {
		log.Fatal(err)
	}

	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	f2, err := os.Open("parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer f2.Close()

	r, err := NewParquetReader(f2)
	if err != nil {
		log.Fatal(err)
	}

	enc := json.NewEncoder(os.Stdout)
	for r.Next() {
		var c C
		r.Scan(&c)
		enc.Encode(c)
	}

	if err := r.Error(); err != nil {
		log.Fatal(err)
	}
}

// Being is split out only to show how embedded structs
// are handled.
type A struct {
	ID int32 `parquet:"id"`
}

type B struct {
	A
	Name string `parquet:"name"`
}

type C struct {
	B
}
