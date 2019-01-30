package main

import (
	"math"
	"math/rand"
	"time"
)

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

	var anv *uint64
	if i%3 == 0 {
		x := math.MaxUint64 - uint64(i*100)
		anv = &x
	}

	return Person{
		Being: Being{
			ID:  int32(i),
			Age: age,
		},
		Happiness:   int64(i * 2),
		Sadness:     sadness,
		Code:        randString(8),
		Funkiness:   rand.Float32(),
		Lameness:    lameness,
		Keen:        keen,
		Birthday:    uint32(i * 1000),
		Anniversary: anv,
	}
}