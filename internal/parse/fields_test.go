package parse_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	sch "github.com/parsyl/parquet/generated"
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
				{Type: "Being", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Being", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
		},
		{
			name: "private fields",
			typ:  "Private",
			expected: []parse.Field{
				{Type: "Private", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Private", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
		},
		{
			name: "nested struct",
			typ:  "Nested",
			expected: []parse.Field{
				{Type: "Nested", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Being", "ID"}, FieldTypes: []string{"Being", "int32"}, ColumnName: "Being.ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required}},
				{Type: "Nested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Being", "Age"}, FieldTypes: []string{"Being", "int32"}, ColumnName: "Being.Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional}},
				{Type: "Nested", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
			errors: []error{},
		},
		{
			name: "nested struct with name that doesn't match the struct type",
			typ:  "Nested2",
			expected: []parse.Field{
				{Type: "Nested2", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Info", "ID"}, FieldTypes: []string{"Being", "int32"}, ColumnName: "Info.ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required}},
				{Type: "Nested2", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Info", "Age"}, FieldTypes: []string{"Being", "int32"}, ColumnName: "Info.Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional}},
				{Type: "Nested2", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
			errors: []error{},
		},
		{
			name: "2 deep nested struct",
			typ:  "DoubleNested",
			expected: []parse.Field{
				{Type: "DoubleNested", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Nested", "Being", "ID"}, FieldTypes: []string{"Nested", "Being", "int32"}, ColumnName: "Nested.Being.ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required, parse.Required}},
				{Type: "DoubleNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Nested", "Being", "Age"}, FieldTypes: []string{"Nested", "Being", "int32"}, ColumnName: "Nested.Being.Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required, parse.Optional}},
				{Type: "DoubleNested", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Nested", "Anniversary"}, FieldTypes: []string{"Nested", "uint64"}, ColumnName: "Nested.Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional}},
			},
			errors: []error{},
		},
		{
			name: "2 deep optional nested struct",
			typ:  "OptionalDoubleNested",
			expected: []parse.Field{
				{Type: "OptionalDoubleNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"OptionalNested", "Being", "ID"}, FieldTypes: []string{"OptionalNested", "Being", "int32"}, ColumnName: "OptionalNested.Being.ID", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional, parse.Required}},
				{Type: "OptionalDoubleNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"OptionalNested", "Being", "Age"}, FieldTypes: []string{"OptionalNested", "Being", "int32"}, ColumnName: "OptionalNested.Being.Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional, parse.Optional}},
				{Type: "OptionalDoubleNested", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"OptionalNested", "Anniversary"}, FieldTypes: []string{"OptionalNested", "uint64"}, ColumnName: "OptionalNested.Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional}},
			},
			errors: []error{},
		},
		{
			name: "optional nested struct",
			typ:  "OptionalNested",
			expected: []parse.Field{
				{Type: "OptionalNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Being", "ID"}, FieldTypes: []string{"Being", "int32"}, ColumnName: "Being.ID", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required}},
				{Type: "OptionalNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Being", "Age"}, FieldTypes: []string{"Being", "int32"}, ColumnName: "Being.Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional}},
				{Type: "OptionalNested", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
			errors: []error{},
		},
		{
			name: "optional nested struct v2",
			typ:  "OptionalNested2",
			expected: []parse.Field{
				{Type: "OptionalNested2", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Being", "Name"}, FieldTypes: []string{"Thing", "string"}, ColumnName: "Being.Name", Category: "stringOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required}},
				{Type: "OptionalNested2", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
			errors: []error{},
		},
		{
			name:   "unsupported fields",
			typ:    "Unsupported",
			errors: []error{fmt.Errorf("unsupported type: Time")},
			expected: []parse.Field{
				{Type: "Unsupported", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Unsupported", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
		},
		{
			name: "unsupported fields mixed in with supported and embedded",
			typ:  "SupportedAndUnsupported",
			expected: []parse.Field{
				{Type: "SupportedAndUnsupported", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldNames: []string{"Happiness"}, FieldTypes: []string{"int64"}, ColumnName: "Happiness", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "SupportedAndUnsupported", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "SupportedAndUnsupported", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "SupportedAndUnsupported", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
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
				{Type: "Person", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Person", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "Person", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldNames: []string{"Happiness"}, FieldTypes: []string{"int64"}, ColumnName: "Happiness", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Person", FieldType: "Int64OptionalField", ParquetType: "Int64Type", TypeName: "*int64", FieldNames: []string{"Sadness"}, FieldTypes: []string{"int64"}, ColumnName: "Sadness", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "Person", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Code"}, FieldTypes: []string{"string"}, ColumnName: "Code", Category: "string", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Person", FieldType: "Float32Field", ParquetType: "Float32Type", TypeName: "float32", FieldNames: []string{"Funkiness"}, FieldTypes: []string{"float32"}, ColumnName: "Funkiness", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Person", FieldType: "Float32OptionalField", ParquetType: "Float32Type", TypeName: "*float32", FieldNames: []string{"Lameness"}, FieldTypes: []string{"float32"}, ColumnName: "Lameness", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "Person", FieldType: "BoolOptionalField", ParquetType: "BoolType", TypeName: "*bool", FieldNames: []string{"Keen"}, FieldTypes: []string{"bool"}, ColumnName: "Keen", Category: "boolOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "Person", FieldType: "Uint32Field", ParquetType: "Uint32Type", TypeName: "uint32", FieldNames: []string{"Birthday"}, FieldTypes: []string{"uint32"}, ColumnName: "Birthday", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Person", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
		},
		{
			name: "embedded preserve order",
			typ:  "NewOrderPerson",
			expected: []parse.Field{
				{Type: "NewOrderPerson", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldNames: []string{"Happiness"}, FieldTypes: []string{"int64"}, ColumnName: "Happiness", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "NewOrderPerson", FieldType: "Int64OptionalField", ParquetType: "Int64Type", TypeName: "*int64", FieldNames: []string{"Sadness"}, FieldTypes: []string{"int64"}, ColumnName: "Sadness", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "NewOrderPerson", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Code"}, FieldTypes: []string{"string"}, ColumnName: "Code", Category: "string", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "NewOrderPerson", FieldType: "Float32Field", ParquetType: "Float32Type", TypeName: "float32", FieldNames: []string{"Funkiness"}, FieldTypes: []string{"float32"}, ColumnName: "Funkiness", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "NewOrderPerson", FieldType: "Float32OptionalField", ParquetType: "Float32Type", TypeName: "*float32", FieldNames: []string{"Lameness"}, FieldTypes: []string{"float32"}, ColumnName: "Lameness", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "NewOrderPerson", FieldType: "BoolOptionalField", ParquetType: "BoolType", TypeName: "*bool", FieldNames: []string{"Keen"}, FieldTypes: []string{"bool"}, ColumnName: "Keen", Category: "boolOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "NewOrderPerson", FieldType: "Uint32Field", ParquetType: "Uint32Type", TypeName: "uint32", FieldNames: []string{"Birthday"}, FieldTypes: []string{"uint32"}, ColumnName: "Birthday", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "NewOrderPerson", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "NewOrderPerson", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
				{Type: "NewOrderPerson", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnName: "Anniversary", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
		},
		{
			name: "tags",
			typ:  "Tagged",
			expected: []parse.Field{
				{Type: "Tagged", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "id", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Tagged", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Name"}, FieldTypes: []string{"string"}, ColumnName: "name", Category: "string", RepetitionTypes: []parse.RepetitionType{parse.Required}},
			},
		},
		{
			name: "omit tag",
			typ:  "IgnoreMe",
			expected: []parse.Field{
				{Type: "IgnoreMe", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "id", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
			},
		},
		{
			name: "repeated",
			typ:  "Slice",
			expected: []parse.Field{
				{Type: "Slice", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "[]int32", FieldNames: []string{"IDs"}, FieldTypes: []string{"int32"}, ColumnName: "ids", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Repeated}},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice2",
			expected: []parse.Field{
				{Type: "Slice2", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "id", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Slice2", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "[]int32", FieldNames: []string{"IDs"}, FieldTypes: []string{"int32"}, ColumnName: "ids", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Repeated}},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice3",
			expected: []parse.Field{
				{Type: "Slice3", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "id", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Slice3", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "[]int32", FieldNames: []string{"IDs"}, FieldTypes: []string{"int32"}, ColumnName: "ids", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Repeated}},
				{Type: "Slice3", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnName: "Age", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			},
		},
		{
			name: "nested and repeated",
			typ:  "Slice4",
			expected: []parse.Field{
				{Type: "Slice4", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "id", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Slice4", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Hobbies", "Name"}, FieldTypes: []string{"Hobby", "string"}, ColumnName: "Hobbies.Name", Category: "stringOptional", RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Required}},
			},
		},
		{
			name: "nested and repeated v2",
			typ:  "Slice5",
			expected: []parse.Field{
				{Type: "Slice5", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "id", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Slice5", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "[]string", FieldNames: []string{"Hobby", "Names"}, FieldTypes: []string{"Hobby2", "string"}, ColumnName: "Hobby.Names", Category: "stringOptional", RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Repeated}},
			},
		},
		{
			name: "repeated and repeated",
			typ:  "Slice6",
			expected: []parse.Field{
				{Type: "Slice6", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnName: "ID", Category: "numeric", RepetitionTypes: []parse.RepetitionType{parse.Required}},
				{Type: "Slice6", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "[]string", FieldNames: []string{"Hobbies", "Names"}, FieldTypes: []string{"Hobby2", "string"}, ColumnName: "Hobbies.Names", Category: "stringOptional", RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Repeated}},
			},
		},
		{
			name: "nested repeated and repeated",
			typ:  "Slice7",
			expected: []parse.Field{
				{Type: "Slice7", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Thing", "ID"}, FieldTypes: []string{"Slice6", "int32"}, ColumnName: "Thing.ID", Category: "numericOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required}},
				{Type: "Slice7", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "[]string", FieldNames: []string{"Thing", "Hobbies", "Names"}, FieldTypes: []string{"Slice6", "Hobby2", "string"}, ColumnName: "Thing.Hobbies.Names", Category: "stringOptional", RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Repeated, parse.Repeated}},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
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

func pint32(i int32) *int32 {
	return &i
}

func prt(rt sch.FieldRepetitionType) *sch.FieldRepetitionType {
	return &rt
}

func pt(t sch.Type) *sch.Type {
	return &t
}
