package main

//go:generate parquetgen -type Person -package main

import (
	"log"
	"math"
	"math/rand"
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

func newPerson(i int) Person {
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

	var keen *bool
	if i%5 == 0 {
		b := true
		keen = &b
	}

	return Person{
		Being: Being{
			ID:  int32(i),
			Age: age,
		},
		Happiness: int64(i * 2),
		Sadness:   sadness,
		Code:      randString(8),
		Funkiness: rand.Float32(),
		Lameness:  lameness,
		Keen:      keen,
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
