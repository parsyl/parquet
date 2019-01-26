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
				`NewInt32Field(func(x Being) int32 { return x.ID }, "id"),`,
				`NewInt32OptionalField(func(x Being) *int32 { return x.Age }, "Age"),`,
			},
		},
		{
			name: "embedded",
			typ:  "Person",
			expected: []string{
				`NewInt32Field(func(x Person) int32 { return x.ID }, "id"),`,
				`NewInt32OptionalField(func(x Person) *int32 { return x.Age }, "Age"),`,
				`NewInt64Field(func(x Person) int64 { return x.Happiness }, "Happiness"),`,
				`NewInt64OptionalField(func(x Person) *int64 { return x.Sadness }, "sadness"),`,
				`NewStringField(func(x Person) string { return x.Code }, "Code"),`,
				`NewFloat32Field(func(x Person) float32 { return x.Funkiness }, "Funkiness"),`,
				`NewFloat32OptionalField(func(x Person) *float32 { return x.Lameness }, "Lameness"),`,
				`NewBoolOptionalField(func(x Person) *bool { return x.Keen }, "Keen"),`,
				`NewUint32Field(func(x Person) uint32 { return x.Birthday }, "Birthday"),`,
				`NewUint64OptionalField(func(x Person) *uint64 { return x.Anniversary }, "Anniversary"),`,
			},
		},
		{
			name: "omit",
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
