package main

//go:generate parquetgen -input main.go -type Person -package main

import (
	"log"
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
		w.Add(newPerson(i))
	}

	if err := w.Write(); err != nil {
		log.Fatal(err)
	}
}

// Being is split out only to show how embedded structs
// are handled.
type Being struct {
	ID  int32  `parquet:"id"`
	Age *int32 `parquet:"age"`
}

type Person struct {
	Being
	Happiness   int64    `parquet:"happiness"`
	Sadness     *int64   `parquet:"sadness"`
	Code        string   `parquet:"code"`
	Funkiness   float32  `parquet:"funkiness"`
	Lameness    *float32 `parquet:"lameness"`
	Keen        *bool    `parquet:"keen"`
	Birthday    uint32   `parquet:"birthday"`
	Anniversary *uint64  `parquet:"anniversary"`

	// Secret will not be part of parquet.
	Secret string `parquet:"-"`
}
