package main

//go:generate parquetgen -type Person -package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
)

func main() {
	f, err := os.Create("people.parquet")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	w := NewParquetWriter(f)
	jf, err := os.Open("people.json")
	if err != nil {
		log.Fatal(err)
	}

	var people []Person
	if err := json.NewDecoder(jf).Decode(&people); err != nil {
		log.Fatal(err)
	}
	fmt.Println("people", len(people))

	for i, person := range people {
		person.Birthday = math.MaxUint32 - uint32(i+1)
		w.Add(person)
	}

	if err := w.Write(); err != nil {
		log.Fatal(err)
	}
}

type Being struct {
	ID  int32
	Age *int32
}

type Person struct {
	Being
	Happiness   int64
	Sadness     *int64
	Code        string
	Funkiness   float32
	Lameness    *float32
	Keen        *bool
	Birthday    uint32
	Anniversary *uint64
}
