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
				`NewInt32Field(func(x Being) int32 { return x.ID }, func(x *Being, v int32) { x.ID = v }, "ID"),`,
				`NewInt32OptionalField(func(x Being) *int32 { return x.Age }, func(x *Being, v *int32) { x.Age = v }, "Age"),`,
			},
		},
		{
			name: "private fields",
			typ:  "Private",
			expected: []string{
				`NewInt32Field(func(x Private) int32 { return x.ID }, func(x *Private, v int32) { x.ID = v }, "ID"),`,
				`NewInt32OptionalField(func(x Private) *int32 { return x.Age }, func(x *Private, v *int32) { x.Age = v }, "Age"),`,
			},
		},
		{
			name: "nested structs",
			typ:  "Nested",
			expected: []string{
				`NewUint64OptionalField(func(x Nested) *uint64 { return x.Anniversary }, func(x *Nested, v *uint64) { x.Anniversary = v }, "Anniversary"),`,
			},
		},
		{
			name: "unsupported fields",
			typ:  "Unsupported",
			expected: []string{
				`NewInt32Field(func(x Unsupported) int32 { return x.ID }, func(x *Unsupported, v int32) { x.ID = v }, "ID"),`,
				`NewInt32OptionalField(func(x Unsupported) *int32 { return x.Age }, func(x *Unsupported, v *int32) { x.Age = v }, "Age"),`,
			},
		},
		{
			name: "unsupported fields mixed in with supported and embedded",
			typ:  "SupportedAndUnsupported",
			expected: []string{
				`NewInt64Field(func(x SupportedAndUnsupported) int64 { return x.Happiness }, func(x *SupportedAndUnsupported, v int64) { x.Happiness = v }, "Happiness"),`,
				`NewInt32Field(func(x SupportedAndUnsupported) int32 { return x.ID }, func(x *SupportedAndUnsupported, v int32) { x.ID = v }, "ID"),`,
				`NewInt32OptionalField(func(x SupportedAndUnsupported) *int32 { return x.Age }, func(x *SupportedAndUnsupported, v *int32) { x.Age = v }, "Age"),`,
				`NewUint64OptionalField(func(x SupportedAndUnsupported) *uint64 { return x.Anniversary }, func(x *SupportedAndUnsupported, v *uint64) { x.Anniversary = v }, "Anniversary"),`,
			},
		},
		{
			name: "embedded",
			typ:  "Person",
			expected: []string{
				`NewInt32Field(func(x Person) int32 { return x.ID }, func(x *Person, v int32) { x.ID = v }, "ID"),`,
				`NewInt32OptionalField(func(x Person) *int32 { return x.Age }, func(x *Person, v *int32) { x.Age = v }, "Age"),`,
				`NewInt64Field(func(x Person) int64 { return x.Happiness }, func(x *Person, v int64) { x.Happiness = v }, "Happiness"),`,
				`NewInt64OptionalField(func(x Person) *int64 { return x.Sadness }, func(x *Person, v *int64) { x.Sadness = v }, "Sadness"),`,
				`NewStringField(func(x Person) string { return x.Code }, func(x *Person, v string) { x.Code = v }, "Code"),`,
				`NewFloat32Field(func(x Person) float32 { return x.Funkiness }, func(x *Person, v float32) { x.Funkiness = v }, "Funkiness"),`,
				`NewFloat32OptionalField(func(x Person) *float32 { return x.Lameness }, func(x *Person, v *float32) { x.Lameness = v }, "Lameness"),`,
				`NewBoolOptionalField(func(x Person) *bool { return x.Keen }, func(x *Person, v *bool) { x.Keen = v }, "Keen"),`,
				`NewUint32Field(func(x Person) uint32 { return x.Birthday }, func(x *Person, v uint32) { x.Birthday = v }, "Birthday"),`,
				`NewUint64OptionalField(func(x Person) *uint64 { return x.Anniversary }, func(x *Person, v *uint64) { x.Anniversary = v }, "Anniversary"),`,
			},
		},
		{
			name: "embedded preserve order",
			typ:  "NewOrderPerson",
			expected: []string{
				`NewInt64Field(func(x NewOrderPerson) int64 { return x.Happiness }, func(x *NewOrderPerson, v int64) { x.Happiness = v }, "Happiness"),`,
				`NewInt64OptionalField(func(x NewOrderPerson) *int64 { return x.Sadness }, func(x *NewOrderPerson, v *int64) { x.Sadness = v }, "Sadness"),`,
				`NewStringField(func(x NewOrderPerson) string { return x.Code }, func(x *NewOrderPerson, v string) { x.Code = v }, "Code"),`,
				`NewFloat32Field(func(x NewOrderPerson) float32 { return x.Funkiness }, func(x *NewOrderPerson, v float32) { x.Funkiness = v }, "Funkiness"),`,
				`NewFloat32OptionalField(func(x NewOrderPerson) *float32 { return x.Lameness }, func(x *NewOrderPerson, v *float32) { x.Lameness = v }, "Lameness"),`,
				`NewBoolOptionalField(func(x NewOrderPerson) *bool { return x.Keen }, func(x *NewOrderPerson, v *bool) { x.Keen = v }, "Keen"),`,
				`NewUint32Field(func(x NewOrderPerson) uint32 { return x.Birthday }, func(x *NewOrderPerson, v uint32) { x.Birthday = v }, "Birthday"),`,
				`NewInt32Field(func(x NewOrderPerson) int32 { return x.ID }, func(x *NewOrderPerson, v int32) { x.ID = v }, "ID"),`,
				`NewInt32OptionalField(func(x NewOrderPerson) *int32 { return x.Age }, func(x *NewOrderPerson, v *int32) { x.Age = v }, "Age"),`,
				`NewUint64OptionalField(func(x NewOrderPerson) *uint64 { return x.Anniversary }, func(x *NewOrderPerson, v *uint64) { x.Anniversary = v }, "Anniversary"),`,
			},
		},
		{
			name: "tags",
			typ:  "Tagged",
			expected: []string{
				`NewInt32Field(func(x Tagged) int32 { return x.ID }, func(x *Tagged, v int32) { x.ID = v }, "id"),`,
				`NewStringField(func(x Tagged) string { return x.Name }, func(x *Tagged, v string) { x.Name = v }, "name"),`,
			},
		},
		{
			name: "omit tag",
			typ:  "IgnoreMe",
			expected: []string{
				`NewInt32Field(func(x IgnoreMe) int32 { return x.ID }, func(x *IgnoreMe, v int32) { x.ID = v }, "id"),`,
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
