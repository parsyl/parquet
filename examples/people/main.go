package main

//go:generate parquetgen -type Person -package main

import (
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

	for i := 0; i < 2000; i++ {
		w.Add(Person{
			ID:       i,
			Birthday: math.MaxUint32 - uint32(i+1),
		})
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
