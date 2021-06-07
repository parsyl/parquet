package parse_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/parsyl/parquet/internal/fields"
	"github.com/parsyl/parquet/internal/parse"
	sch "github.com/parsyl/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestField(t *testing.T) {
	type testInput struct {
		f        fields.Field
		expected []string
	}

	testCases := []testInput{
		{
			f: fields.Field{FieldName: "First", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Name", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Friends", RepetitionType: fields.Repeated}}},
			expected: []string{
				"Friends",
				"Friends.Name.First",
			},
		},
		{
			f: fields.Field{FieldName: "First", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Name", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Friends", RepetitionType: fields.Required}}},
			expected: []string{
				"Friend.Name.First",
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			if !assert.Equal(t, len(tc.expected), tc.f.MaxDef()) {
				return
			}

			for i := 0; i < tc.f.MaxDef(); i++ {
				s, _, _, _ := tc.f.NilField(i)
				assert.Equal(t, tc.expected[i], s)
			}
		})
	}
}

func TestFields(t *testing.T) {

	type testInput struct {
		name     string
		typ      string
		expected fields.Field
		errors   []error
	}

	testCases := []testInput{
		{
			name: "flat",
			typ:  "Being",
			expected: fields.Field{
				Type:       "Being",
				FieldName:  "Being",
				FieldType:  "Being",
				ColumnName: "Being",
				TypeName:   "Being",
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "private fields",
			typ:  "Private",
			expected: fields.Field{
				Type:       "Private",
				FieldName:  "Private",
				FieldType:  "Private",
				ColumnName: "Private",
				TypeName:   "Private",
				Children: []fields.Field{
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "nested struct",
			typ:  "Nested",
			expected: fields.Field{
				Type:       "Nested",
				FieldName:  "Nested",
				FieldType:  "Nested",
				ColumnName: "Nested",
				TypeName:   "Nested",
				Children: []fields.Field{
					{Type: "Being", TypeName: "Being", FieldName: "Being", FieldType: "Being", ColumnName: "Being", RepetitionType: fields.Required, Children: []fields.Field{
						{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
						{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
					}},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name: "nested struct with name that doesn't match the struct type",
			typ:  "Nested2",
			expected: fields.Field{
				Type:       "Nested2",
				FieldName:  "Nested2",
				FieldType:  "Nested2",
				ColumnName: "Nested2",
				TypeName:   "Nested2",
				Children: []fields.Field{
					{Type: "Being", TypeName: "Being", FieldName: "Info", FieldType: "Being", ColumnName: "Info", RepetitionType: fields.Required, Children: []fields.Field{
						{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
						{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
					}},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name: "2 deep nested struct",
			typ:  "DoubleNested",
			expected: fields.Field{
				Type:       "DoubleNested",
				FieldName:  "DoubleNested",
				FieldType:  "DoubleNested",
				ColumnName: "DoubleNested",
				TypeName:   "DoubleNested",
				Children: []fields.Field{
					{
						Type:       "Nested",
						FieldName:  "Nested",
						FieldType:  "Nested",
						ColumnName: "Nested",
						TypeName:   "Nested",
						Children: []fields.Field{
							{Type: "Being", TypeName: "Being", FieldName: "Being", FieldType: "Being", ColumnName: "Being", RepetitionType: fields.Required, Children: []fields.Field{
								{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
								{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
							}},
							{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
						},
					},
				},
			},
			errors: []error{},
		},
		{
			name: "2 deep optional nested struct",
			typ:  "OptionalDoubleNested",
			expected: fields.Field{
				Type:       "OptionalDoubleNested",
				FieldName:  "OptionalDoubleNested",
				FieldType:  "OptionalDoubleNested",
				ColumnName: "OptionalDoubleNested",
				TypeName:   "OptionalDoubleNested",
				Children: []fields.Field{
					{
						Type:       "OptionalNested",
						FieldName:  "OptionalNested",
						FieldType:  "OptionalNested",
						ColumnName: "OptionalNested",
						TypeName:   "OptionalNested",
						Children: []fields.Field{
							{Type: "Being", TypeName: "*Being", FieldName: "Being", FieldType: "Being", ColumnName: "Being", RepetitionType: fields.Optional, Children: []fields.Field{
								{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
								{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
							}},
							{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
						},
					},
				},
			},
			errors: []error{},
		},
		// {
		// 	name: "optional nested struct",
		// 	typ:  "OptionalNested",
		// 	expected: []fields.Field{
		// 		{Type: "OptionalNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Being", "ID"}, FieldTypes: []string{"Being", "int32"}, ColumnNames: []string{"Being", "ID"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
		// 		{Type: "OptionalNested", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Being", "Age"}, FieldTypes: []string{"Being", "int32"}, ColumnNames: []string{"Being", "Age"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
		// 		{Type: "OptionalNested", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnNames: []string{"Anniversary"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// 	errors: []error{},
		// },
		// {
		// 	name: "optional nested struct v2",
		// 	typ:  "OptionalNested2",
		// 	expected: []fields.Field{
		// 		{Type: "OptionalNested2", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Being", "Name"}, FieldTypes: []string{"Thing", "string"}, ColumnNames: []string{"Being", "Name"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
		// 		{Type: "OptionalNested2", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnNames: []string{"Anniversary"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// 	errors: []error{},
		// },
		// {
		// 	name:   "unsupported fields",
		// 	typ:    "Unsupported",
		// 	errors: []error{fmt.Errorf("unsupported type: Time")},
		// 	expected: []fields.Field{
		// 		{Type: "Unsupported", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"ID"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Unsupported", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"Age"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// },
		// {
		// 	name: "unsupported fields mixed in with supported and embedded",
		// 	typ:  "SupportedAndUnsupported",
		// 	expected: []fields.Field{
		// 		{Type: "SupportedAndUnsupported", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldNames: []string{"Happiness"}, FieldTypes: []string{"int64"}, ColumnNames: []string{"Happiness"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "SupportedAndUnsupported", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"ID"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "SupportedAndUnsupported", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"Age"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "SupportedAndUnsupported", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnNames: []string{"Anniversary"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// 	errors: []error{
		// 		fmt.Errorf("unsupported type: T1"),
		// 		fmt.Errorf("unsupported type: T2"),
		// 	},
		// },
		// {
		// 	name: "embedded",
		// 	typ:  "Person",
		// 	expected: []fields.Field{
		// 		{Type: "Person", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"ID"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Person", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"Age"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "Person", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldNames: []string{"Happiness"}, FieldTypes: []string{"int64"}, ColumnNames: []string{"Happiness"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Person", FieldType: "Int64OptionalField", ParquetType: "Int64Type", TypeName: "*int64", FieldNames: []string{"Sadness"}, FieldTypes: []string{"int64"}, ColumnNames: []string{"Sadness"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "Person", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Code"}, FieldTypes: []string{"string"}, ColumnNames: []string{"Code"}, Category: "string", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Person", FieldType: "Float32Field", ParquetType: "Float32Type", TypeName: "float32", FieldNames: []string{"Funkiness"}, FieldTypes: []string{"float32"}, ColumnNames: []string{"Funkiness"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Person", FieldType: "Float32OptionalField", ParquetType: "Float32Type", TypeName: "*float32", FieldNames: []string{"Lameness"}, FieldTypes: []string{"float32"}, ColumnNames: []string{"Lameness"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "Person", FieldType: "BoolOptionalField", ParquetType: "BoolType", TypeName: "*bool", FieldNames: []string{"Keen"}, FieldTypes: []string{"bool"}, ColumnNames: []string{"Keen"}, Category: "boolOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "Person", FieldType: "Uint32Field", ParquetType: "Uint32Type", TypeName: "uint32", FieldNames: []string{"Birthday"}, FieldTypes: []string{"uint32"}, ColumnNames: []string{"Birthday"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Person", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnNames: []string{"Anniversary"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// },
		// {
		// 	name: "embedded preserve order",
		// 	typ:  "NewOrderPerson",
		// 	expected: []fields.Field{
		// 		{Type: "NewOrderPerson", FieldType: "Int64Field", ParquetType: "Int64Type", TypeName: "int64", FieldNames: []string{"Happiness"}, FieldTypes: []string{"int64"}, ColumnNames: []string{"Happiness"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "NewOrderPerson", FieldType: "Int64OptionalField", ParquetType: "Int64Type", TypeName: "*int64", FieldNames: []string{"Sadness"}, FieldTypes: []string{"int64"}, ColumnNames: []string{"Sadness"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "NewOrderPerson", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Code"}, FieldTypes: []string{"string"}, ColumnNames: []string{"Code"}, Category: "string", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "NewOrderPerson", FieldType: "Float32Field", ParquetType: "Float32Type", TypeName: "float32", FieldNames: []string{"Funkiness"}, FieldTypes: []string{"float32"}, ColumnNames: []string{"Funkiness"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "NewOrderPerson", FieldType: "Float32OptionalField", ParquetType: "Float32Type", TypeName: "*float32", FieldNames: []string{"Lameness"}, FieldTypes: []string{"float32"}, ColumnNames: []string{"Lameness"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "NewOrderPerson", FieldType: "BoolOptionalField", ParquetType: "BoolType", TypeName: "*bool", FieldNames: []string{"Keen"}, FieldTypes: []string{"bool"}, ColumnNames: []string{"Keen"}, Category: "boolOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "NewOrderPerson", FieldType: "Uint32Field", ParquetType: "Uint32Type", TypeName: "uint32", FieldNames: []string{"Birthday"}, FieldTypes: []string{"uint32"}, ColumnNames: []string{"Birthday"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "NewOrderPerson", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"ID"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "NewOrderPerson", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"Age"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 		{Type: "NewOrderPerson", FieldType: "Uint64OptionalField", ParquetType: "Uint64Type", TypeName: "*uint64", FieldNames: []string{"Anniversary"}, FieldTypes: []string{"uint64"}, ColumnNames: []string{"Anniversary"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// },
		// {
		// 	name: "tags",
		// 	typ:  "Tagged",
		// 	expected: []fields.Field{
		// 		{Type: "Tagged", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Tagged", FieldType: "StringField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Name"}, FieldTypes: []string{"string"}, ColumnNames: []string{"name"}, Category: "string", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 	},
		// },
		// {
		// 	name: "omit tag",
		// 	typ:  "IgnoreMe",
		// 	expected: []fields.Field{
		// 		{Type: "IgnoreMe", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 	},
		// },
		{
			name: "repeated",
			typ:  "Slice",
			expected: fields.Field{
				Type:       "Slice",
				FieldName:  "Slice",
				FieldType:  "Slice",
				ColumnName: "Slice",
				TypeName:   "Slice",
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "IDs", ColumnName: "ids", Category: "numericOptional", RepetitionType: fields.Repeated},
				},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice2",
			expected: fields.Field{
				Type:       "Slice2",
				FieldName:  "Slice2",
				FieldType:  "Slice2",
				ColumnName: "Slice2",
				TypeName:   "Slice2",
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "IDs", ColumnName: "ids", Category: "numericOptional", RepetitionType: fields.Repeated},
				},
			},
			// expected: []fields.Field{
			// 	{Type: "Slice2", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
			// 	{Type: "Slice2", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "[]int32", FieldNames: []string{"IDs"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"ids"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
			// },
		},
		// {
		// 	name: "repeated v2",
		// 	typ:  "Slice3",
		// 	expected: []fields.Field{
		// 		{Type: "Slice3", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Slice3", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "[]int32", FieldNames: []string{"IDs"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"ids"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
		// 		{Type: "Slice3", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "*int32", FieldNames: []string{"Age"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"Age"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional}},
		// 	},
		// },
		// {
		// 	name: "nested and repeated",
		// 	typ:  "Slice4",
		// 	expected: []fields.Field{
		// 		{Type: "Slice4", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Slice4", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "string", FieldNames: []string{"Hobbies", "Name"}, FieldTypes: []string{"Hobby", "string"}, ColumnNames: []string{"Hobbies", "Name"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required}},
		// 	},
		// },
		// {
		// 	name: "nested and repeated v2",
		// 	typ:  "Slice5",
		// 	expected: []fields.Field{
		// 		{Type: "Slice5", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Slice5", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "[]string", FieldNames: []string{"Hobby", "Names"}, FieldTypes: []string{"Hobby2", "string"}, ColumnNames: []string{"hobby", "names"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated}},
		// 	},
		// },
		// {
		// 	name: "repeated and repeated",
		// 	typ:  "Slice6",
		// 	expected: []fields.Field{
		// 		{Type: "Slice6", FieldType: "Int32Field", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, ColumnNames: []string{"id"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{fields.Required}},
		// 		{Type: "Slice6", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "[]string", FieldNames: []string{"Hobbies", "Names"}, FieldTypes: []string{"Hobby2", "string"}, ColumnNames: []string{"hobbies", "names"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated}},
		// 	},
		// },
		// {
		// 	name: "nested repeated and repeated",
		// 	typ:  "Slice7",
		// 	expected: []fields.Field{
		// 		{Type: "Slice7", FieldType: "Int32OptionalField", ParquetType: "Int32Type", TypeName: "int32", FieldNames: []string{"Thing", "ID"}, FieldTypes: []string{"Slice6", "int32"}, ColumnNames: []string{"thing", "id"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
		// 		{Type: "Slice7", FieldType: "StringOptionalField", ParquetType: "StringType", TypeName: "[]string", FieldNames: []string{"Thing", "Hobbies", "Names"}, FieldTypes: []string{"Slice6", "Hobby2", "string"}, ColumnNames: []string{"thing", "hobbies", "names"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated, fields.Repeated}},
		// 	},
		// },
		// {
		// 	name: "dremel paper example",
		// 	typ:  "Document",
		// 	expected: []fields.Field{
		// 		{Type: "Document", FieldNames: []string{"DocID"}, FieldTypes: []string{"int64"}, TypeName: "int64", FieldType: "Int64Field", ParquetType: "Int64Type", ColumnNames: []string{"DocID"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{0}},
		// 		{Type: "Document", FieldNames: []string{"Links", "Backward"}, FieldTypes: []string{"Link", "int64"}, TypeName: "[]int64", FieldType: "Int64OptionalField", ParquetType: "Int64Type", ColumnNames: []string{"Links", "Backward"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{2, 2}},
		// 		{Type: "Document", FieldNames: []string{"Links", "Forward"}, FieldTypes: []string{"Link", "int64"}, TypeName: "[]int64", FieldType: "Int64OptionalField", ParquetType: "Int64Type", ColumnNames: []string{"Links", "Forward"}, Category: "numericOptional", RepetitionTypes: []fields.RepetitionType{2, 2}},
		// 		{Type: "Document", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, TypeName: "string", FieldType: "StringOptionalField", ParquetType: "StringType", ColumnNames: []string{"Names", "Languages", "Code"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{2, 2, 0}},
		// 		{Type: "Document", FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, TypeName: "*string", FieldType: "StringOptionalField", ParquetType: "StringType", ColumnNames: []string{"Names", "Languages", "Country"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{2, 2, 1}},
		// 		{Type: "Document", FieldNames: []string{"Names", "URL"}, FieldTypes: []string{"Name", "string"}, TypeName: "*string", FieldType: "StringOptionalField", ParquetType: "StringType", ColumnNames: []string{"Names", "URL"}, Category: "stringOptional", RepetitionTypes: []fields.RepetitionType{2, 1}},
		// 	},
		// },
		// {
		// 	name: "embedded embedded embedded",
		// 	typ:  "A",
		// 	expected: []fields.Field{
		// 		{Type: "A", FieldNames: []string{"D"}, FieldTypes: []string{"int32"}, TypeName: "int32", FieldType: "Int32Field", ParquetType: "Int32Type", ColumnNames: []string{"D"}, Category: "numeric", RepetitionTypes: []fields.RepetitionType{0}},
		// 		{Type: "A", FieldNames: []string{"C"}, FieldTypes: []string{"string"}, TypeName: "string", FieldType: "StringField", ParquetType: "StringType", ColumnNames: []string{"C"}, Category: "string", RepetitionTypes: []fields.RepetitionType{0}},
		// 		{Type: "A", FieldNames: []string{"B"}, FieldTypes: []string{"bool"}, TypeName: "bool", FieldType: "BoolField", ParquetType: "BoolType", ColumnNames: []string{"B"}, Category: "bool", RepetitionTypes: []fields.RepetitionType{0}},
		// 		{Type: "A", FieldNames: []string{"Name"}, FieldTypes: []string{"string"}, TypeName: "string", FieldType: "StringField", ParquetType: "StringType", ColumnNames: []string{"Name"}, Category: "string", RepetitionTypes: []fields.RepetitionType{0}},
		// 	},
		// },
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			out, err := parse.Fields(tc.typ, "./parse_test.go")
			assert.Nil(t, err, tc.name)
			assert.Equal(t, tc.expected, out.Parent, tc.name)
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

// func TestDefIndex(t *testing.T) {
// 	testCases := []struct {
// 		def      int
// 		field    fields.Field
// 		expected int
// 	}{
// 		{
// 			def:      1,
// 			field:    fields.Field{RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Repeated}},
// 			expected: 1,
// 		},
// 		{
// 			def:      2,
// 			field:    fields.Field{RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Repeated}},
// 			expected: 2,
// 		},
// 		{
// 			def:      1,
// 			field:    fields.Field{RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required, fields.Repeated}},
// 			expected: 0,
// 		},
// 		{
// 			def:      2,
// 			field:    fields.Field{RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required, fields.Repeated}},
// 			expected: 2,
// 		},
// 		{
// 			def:      2,
// 			field:    fields.Field{RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Optional, fields.Required}},
// 			expected: 1,
// 		},
// 		{
// 			def:      1,
// 			field:    fields.Field{RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Optional, fields.Required}},
// 			expected: 0,
// 		},
// 	}

// 	for i, tc := range testCases {
// 		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
// 			assert.Equal(t, tc.expected, tc.field.DefIndex(tc.def))
// 		})
// 	}
// }
