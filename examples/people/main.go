package main

//go:generate parquetgen -input main.go -type Person -package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	rd = flag.String("read", "", "read a parquet file")
)

func main() {
	flag.Parse()
	if *rd != "" {
		read()
	} else {
		write()
	}
}

func write() {
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

func read() {
	f, err := os.Open(*rd)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r, err := NewParquetReader(f)
	if err != nil {
		log.Fatal(err)
	}

	for r.Next() {
		var p Person
		err := r.Scan(&p)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%+v\n", p)
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
