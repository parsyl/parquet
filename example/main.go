package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

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

	rec := p.New(f, p.MaxPageSize(10000))

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

		var sadness *int64
		if i%3 == 0 {
			s := int64(i + 5)
			sadness = &s
		}

		var lameness *float32
		if i%4 == 0 {
			l := rand.Float32()
			lameness = &l
		}
		records = append(records, p.Record{
			ID:        int32(i),
			Age:       age,
			Happiness: int64(i * 2),
			Sadness:   sadness,
			Code:      randString(8),
			Funkiness: rand.Float32(),
			Lameness:  lameness,
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
