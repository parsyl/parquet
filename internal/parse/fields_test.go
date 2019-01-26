package parse_test

import (
	"testing"

	"github.com/parsyl/parquet/internal/parse"
	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {

	type testInput struct {
		name     string
		typ      string
		expected []string
	}

	testCases := []testInput{
		{
			name: "flat",
			typ:  "Being",
			expected: []string{
				`NewInt32Field(func(x Being) int32 { return x.ID }, "ID"),`,
				`NewInt32OptionalField(func(x Being) *int32 { return x.Age }, "Age"),`,
			},
		},
		{
			name: "embedded",
			typ:  "Person",
			expected: []string{
				`NewInt32Field(func(x Person) int32 { return x.ID }, "ID"),`,
				`NewInt32OptionalField(func(x Person) *int32 { return x.Age }, "Age"),`,
				`NewInt64Field(func(x Person) int64 { return x.Happiness }, "Happiness"),`,
				`NewInt64OptionalField(func(x Person) *int64 { return x.Sadness }, "Sadness"),`,
				`NewStringField(func(x Person) string { return x.Code }, "Code"),`,
				`NewFloat32Field(func(x Person) float32 { return x.Funkiness }, "Funkiness"),`,
				`NewFloat32OptionalField(func(x Person) *float32 { return x.Lameness }, "Lameness"),`,
				`NewBoolOptionalField(func(x Person) *bool { return x.Keen }, "Keen"),`,
				`NewUint32Field(func(x Person) uint32 { return x.Birthday }, "Birthday"),`,
				`NewUint64OptionalField(func(x Person) *uint64 { return x.Anniversary }, "Anniversary"),`,
			},
		},
		{
			name: "embedded preserve order",
			typ:  "NewOrderPerson",
			expected: []string{
				`NewInt64Field(func(x NewOrderPerson) int64 { return x.Happiness }, "Happiness"),`,
				`NewInt64OptionalField(func(x NewOrderPerson) *int64 { return x.Sadness }, "Sadness"),`,
				`NewStringField(func(x NewOrderPerson) string { return x.Code }, "Code"),`,
				`NewFloat32Field(func(x NewOrderPerson) float32 { return x.Funkiness }, "Funkiness"),`,
				`NewFloat32OptionalField(func(x NewOrderPerson) *float32 { return x.Lameness }, "Lameness"),`,
				`NewBoolOptionalField(func(x NewOrderPerson) *bool { return x.Keen }, "Keen"),`,
				`NewUint32Field(func(x NewOrderPerson) uint32 { return x.Birthday }, "Birthday"),`,
				`NewInt32Field(func(x NewOrderPerson) int32 { return x.ID }, "ID"),`,
				`NewInt32OptionalField(func(x NewOrderPerson) *int32 { return x.Age }, "Age"),`,
				`NewUint64OptionalField(func(x NewOrderPerson) *uint64 { return x.Anniversary }, "Anniversary"),`,
			},
		},
		{
			name: "tags",
			typ:  "Tagged",
			expected: []string{
				`NewInt32Field(func(x Tagged) int32 { return x.ID }, "id"),`,
				`NewStringField(func(x Tagged) string { return x.Name }, "name"),`,
			},
		},
		{
			name: "omit tag",
			typ:  "IgnoreMe",
			expected: []string{
				`NewInt32Field(func(x IgnoreMe) int32 { return x.ID }, "id"),`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(subT *testing.T) {
			out, err := parse.Fields(tc.typ, "./parse_test.go")
			assert.Nil(subT, err, tc.name)
			assert.Equal(subT, tc.expected, out, tc.name)
		})
	}
}
