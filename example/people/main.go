package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/cswank/parquet"
)

func main() {
	f, err := os.Create("people.parquet")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	w := NewParquetWriter(
		f,
		[]Field{
			NewInt32Field(func(p Person) int32 { return p.ID }, "id"),
			NewInt32OptionalField(func(p Person) *int32 { return p.Age }, "age"),
			NewInt64Field(func(p Person) int64 { return p.Happiness }, "happiness"),
			NewInt64OptionalField(func(p Person) *int64 { return p.Sadness }, "sadness"),
			NewStringField(func(p Person) string { return p.Code }, "code"),
			NewFloat32Field(func(p Person) float32 { return p.Funkiness }, "funkiness"),
			NewFloat32OptionalField(func(p Person) *float32 { return p.Lameness }, "lameness"),
			NewOptionalBoolField(func(p Person) *bool { return p.Keen }, "keen"),
		},
		parquet.New(
			parquet.Field{Name: "id", Type: parquet.Int32Type, RepetitionType: parquet.RepetitionRequired},
			parquet.Field{Name: "age", Type: parquet.Int32Type, RepetitionType: parquet.RepetitionOptional},
			parquet.Field{Name: "happiness", Type: parquet.Int64Type, RepetitionType: parquet.RepetitionRequired},
			parquet.Field{Name: "sadness", Type: parquet.Int64Type, RepetitionType: parquet.RepetitionOptional},
			parquet.Field{Name: "code", Type: parquet.StringType, RepetitionType: parquet.RepetitionRequired},
			parquet.Field{Name: "funkiness", Type: parquet.Float32Type, RepetitionType: parquet.RepetitionRequired},
			parquet.Field{Name: "lameness", Type: parquet.Float32Type, RepetitionType: parquet.RepetitionOptional},
			parquet.Field{Name: "keen", Type: parquet.BoolType, RepetitionType: parquet.RepetitionOptional},
		),
	)

	jf, err := os.Open("people.json")
	if err != nil {
		log.Fatal(err)
	}

	var people []Person
	if err := json.NewDecoder(jf).Decode(&people); err != nil {
		log.Fatal(err)
	}

	for _, person := range people {
		w.Add(person)
	}

	if err := w.Write(); err != nil {
		log.Fatal(err)
	}
}

type Person struct {
	ID        int32
	Age       *int32
	Happiness int64
	Sadness   *int64
	Code      string
	Funkiness float32
	Lameness  *float32
	Keen      *bool
}
