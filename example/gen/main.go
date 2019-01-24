package main

import (
	"log"
	"os"

	"github.com/cswank/parquet"
	"github.com/cswank/parquet/schema"
)

func main() {
	f, err := os.Create("example.parquet")
	if err != nil {
		log.Fatal(err)
	}

	r := parquet.New(
		f,
		parquet.Fields([]parquet.Fields{
			parquet.NewInt32Field(func(r Record) int32 { return r.ID }, "id"),
			parquet.NewInt32OptionalField(func(r Record) *int32 { return r.Age }, "age"),
			parquet.NewInt64Field(func(r Record) int64 { return r.Happiness }, "happiness"),
			parquet.NewInt64OptionalField(func(r Record) *int64 { return r.Sadness }, "sadness"),
			parquet.NewStringField(func(r Record) string { return r.Code }, "code"),
			parquet.NewFloat32Field(func(r Record) float32 { return r.Funkiness }, "funkiness"),
			parquet.NewFloat32OptionalField(func(r Record) *float32 { return r.Lameness }, "lameness"),
			parquet.NewOptionalBoolField(func(r Record) *bool { return r.Keen }, "keen"),
		}),
		parquet.Schema([]schema.Field{
			schema.Field{Name: "id", Type: schema.Int32Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "age", Type: schema.Int32Type, RepetitionType: schema.RepetitionOptional},
			schema.Field{Name: "happiness", Type: schema.Int64Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "sadness", Type: schema.Int64Type, RepetitionType: schema.RepetitionOptional},
			schema.Field{Name: "code", Type: schema.StringType, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "funkiness", Type: schema.Float32Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "lameness", Type: schema.Float32Type, RepetitionType: schema.RepetitionOptional},
			schema.Field{Name: "keen", Type: schema.BoolType, RepetitionType: schema.RepetitionOptional},
		}),
	)
}

type Record struct {
	ID        int32
	Age       *int32
	Happiness int64
	Sadness   *int64
	Code      string
	Funkiness float32
	Lameness  *float32
	Keen      *bool
}
