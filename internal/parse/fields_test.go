package parse_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/parsyl/parquet/internal/parse"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestFields(t *testing.T) {

	type testInput struct {
		name     string
		typ      string
		expected []parse.Field
		errors   []error
	}

	testCases := []testInput{
		{
			name: "flat",
			typ:  "Being",
			expected: []parse.Field{
				{Type: "Being", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric"},
				{Type: "Being", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional"},
			},
		},
		{
			name: "private fields",
			typ:  "Private",
			expected: []parse.Field{
				{Type: "Private", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric"},
				{Type: "Private", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional"},
			},
		},
		{
			name: "nested structs",
			typ:  "Nested",
			expected: []parse.Field{
				{Type: "Nested", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", ColumnName: "Anniversary", Category: "numericOptional"},
			},
			errors: []error{
				fmt.Errorf("unsupported type: Being"),
			},
		},
		{
			name:   "unsupported fields",
			typ:    "Unsupported",
			errors: []error{fmt.Errorf("unsupported type: Time")},
			expected: []parse.Field{
				{Type: "Unsupported", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric"},
				{Type: "Unsupported", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional"},
			},
		},
		{
			name: "unsupported fields mixed in with supported and embedded",
			typ:  "SupportedAndUnsupported",
			expected: []parse.Field{
				{Type: "SupportedAndUnsupported", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldName: "Happiness", ColumnName: "Happiness", Category: "numeric"},
				{Type: "SupportedAndUnsupported", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric"},
				{Type: "SupportedAndUnsupported", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional"},
				{Type: "SupportedAndUnsupported", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", ColumnName: "Anniversary", Category: "numericOptional"},
			},
			errors: []error{
				fmt.Errorf("unsupported type: T1"),
				fmt.Errorf("unsupported type: T2"),
			},
		},
		{
			name: "embedded",
			typ:  "Person",
			expected: []parse.Field{
				{Type: "Person", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric"},
				{Type: "Person", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional"},
				{Type: "Person", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldName: "Happiness", ColumnName: "Happiness", Category: "numeric"},
				{Type: "Person", FieldType: "Int64OptionalField", ParquetType: "Int64Type", TypeName: "*int64", FieldName: "Sadness", ColumnName: "Sadness", Category: "numericOptional"},
				{Type: "Person", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldName: "Code", ColumnName: "Code", Category: "string"},
				{Type: "Person", FieldType: "Float32Field", ParquetType: "Float32Type", TypeName: "float32", FieldName: "Funkiness", ColumnName: "Funkiness", Category: "numeric"},
				{Type: "Person", FieldType: "Float32OptionalField", ParquetType: "Float32Type", TypeName: "*float32", FieldName: "Lameness", ColumnName: "Lameness", Category: "numericOptional"},
				{Type: "Person", FieldType: "BoolOptionalField", ParquetType: "BoolType", TypeName: "*bool", FieldName: "Keen", ColumnName: "Keen", Category: "boolOptional"},
				{Type: "Person", FieldType: "Uint32Field", ParquetType: "Uint32Type", TypeName: "uint32", FieldName: "Birthday", ColumnName: "Birthday", Category: "numeric"},
				{Type: "Person", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", ColumnName: "Anniversary", Category: "numericOptional"},
			},
		},
		{
			name: "embedded preserve order",
			typ:  "NewOrderPerson",
			expected: []parse.Field{
				{Type: "NewOrderPerson", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldName: "Happiness", ColumnName: "Happiness", Category: "numeric"},
				{Type: "NewOrderPerson", FieldType: "Int64OptionalField", ParquetType: "Int64Type", TypeName: "*int64", FieldName: "Sadness", ColumnName: "Sadness", Category: "numericOptional"},
				{Type: "NewOrderPerson", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldName: "Code", ColumnName: "Code", Category: "string"},
				{Type: "NewOrderPerson", FieldType: "Float32Field", ParquetType: "Float32Type", TypeName: "float32", FieldName: "Funkiness", ColumnName: "Funkiness", Category: "numeric"},
				{Type: "NewOrderPerson", FieldType: "Float32OptionalField", ParquetType: "Float32Type", TypeName: "*float32", FieldName: "Lameness", ColumnName: "Lameness", Category: "numericOptional"},
				{Type: "NewOrderPerson", FieldType: "BoolOptionalField", ParquetType: "BoolType", TypeName: "*bool", FieldName: "Keen", ColumnName: "Keen", Category: "boolOptional"},
				{Type: "NewOrderPerson", FieldType: "Uint32Field", ParquetType: "Uint32Type", TypeName: "uint32", FieldName: "Birthday", ColumnName: "Birthday", Category: "numeric"},
				{Type: "NewOrderPerson", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric"},
				{Type: "NewOrderPerson", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional"},
				{Type: "NewOrderPerson", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", ColumnName: "Anniversary", Category: "numericOptional"},
			},
		},
		{
			name: "tags",
			typ:  "Tagged",
			expected: []parse.Field{
				{Type: "Tagged", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric"},
				{Type: "Tagged", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldName: "Name", ColumnName: "name", Category: "string"},
			},
		},
		{
			name: "omit tag",
			typ:  "IgnoreMe",
			expected: []parse.Field{
				{Type: "IgnoreMe", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := parse.Fields(tc.typ, "./parse_test.go")
			assert.Nil(t, err, tc.name)
			assert.Equal(t, tc.expected, out.Fields, tc.name)
			if assert.Equal(t, len(tc.errors), len(out.Errors), tc.name) {
				for i, err := range out.Errors {
					assert.EqualError(t, tc.errors[i], err.Error(), tc.name)
				}
			} else {
				for _, err := range out.Errors {
					fmt.Println(err)
				}
			}
		})
	}
}
