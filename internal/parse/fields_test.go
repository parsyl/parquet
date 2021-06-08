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
				Children: []fields.Field{
					{
						Type: "Nested", FieldName: "Nested", FieldType: "Nested", ColumnName: "Nested", TypeName: "Nested",
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
				Children: []fields.Field{
					{
						Type: "OptionalNested", FieldName: "OptionalNested", FieldType: "OptionalNested", ColumnName: "OptionalNested", TypeName: "OptionalNested",
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
		{
			name: "optional nested struct",
			typ:  "OptionalNested",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Being", TypeName: "*Being", FieldName: "Being", FieldType: "Being", ColumnName: "Being", RepetitionType: fields.Optional, Children: []fields.Field{
						{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
						{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
					}},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name: "optional nested struct v2",
			typ:  "OptionalNested2",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Thing", TypeName: "*Thing", FieldName: "Being", FieldType: "Thing", ColumnName: "Being", RepetitionType: fields.Optional, Children: []fields.Field{
						{ParquetType: "StringType", TypeName: "string", FieldName: "Name", FieldType: "string", ColumnName: "Name", Category: "string", RepetitionType: fields.Required},
					}},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name:   "unsupported fields",
			typ:    "Unsupported",
			errors: []error{fmt.Errorf("unsupported type &{time Time}")},
			expected: fields.Field{
				Children: []fields.Field{
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "unsupported fields mixed in with supported and embedded",
			typ:  "SupportedAndUnsupported",
			expected: fields.Field{
				Children: []fields.Field{
					{ParquetType: "Int64Type", TypeName: "int64", FieldName: "Happiness", FieldType: "int64", ColumnName: "Happiness", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
			errors: []error{
				fmt.Errorf("unsupported type &{time Time}"),
				fmt.Errorf("unsupported type &{time Time}"),
			},
		},
		{
			name: "embedded",
			typ:  "Person",
			expected: fields.Field{
				Children: []fields.Field{
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "Int64Type", TypeName: "int64", FieldName: "Happiness", FieldType: "int64", ColumnName: "Happiness", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int64Type", TypeName: "*int64", FieldName: "Sadness", FieldType: "int64", ColumnName: "Sadness", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "StringType", TypeName: "string", FieldName: "Code", FieldType: "string", ColumnName: "Code", Category: "string", RepetitionType: fields.Required},
					{ParquetType: "Float32Type", TypeName: "float32", FieldType: "float32", FieldName: "Funkiness", ColumnName: "Funkiness", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Float32Type", TypeName: "*float32", FieldType: "float32", FieldName: "Lameness", ColumnName: "Lameness", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "BoolType", TypeName: "*bool", FieldType: "bool", FieldName: "Keen", ColumnName: "Keen", Category: "boolOptional", RepetitionType: fields.Optional},
					{ParquetType: "Uint32Type", TypeName: "uint32", FieldType: "uint32", FieldName: "Birthday", ColumnName: "Birthday", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "embedded preserve order",
			typ:  "NewOrderPerson",
			expected: fields.Field{
				Children: []fields.Field{
					{ParquetType: "Int64Type", TypeName: "int64", FieldName: "Happiness", FieldType: "int64", ColumnName: "Happiness", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int64Type", TypeName: "*int64", FieldName: "Sadness", FieldType: "int64", ColumnName: "Sadness", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "StringType", TypeName: "string", FieldName: "Code", FieldType: "string", ColumnName: "Code", Category: "string", RepetitionType: fields.Required},
					{ParquetType: "Float32Type", TypeName: "float32", FieldType: "float32", FieldName: "Funkiness", ColumnName: "Funkiness", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Float32Type", TypeName: "*float32", FieldType: "float32", FieldName: "Lameness", ColumnName: "Lameness", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "BoolType", TypeName: "*bool", FieldType: "bool", FieldName: "Keen", ColumnName: "Keen", Category: "boolOptional", RepetitionType: fields.Optional},
					{ParquetType: "Uint32Type", TypeName: "uint32", FieldType: "uint32", FieldName: "Birthday", ColumnName: "Birthday", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "ID", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
					{ParquetType: "Uint64Type", TypeName: "*uint64", FieldName: "Anniversary", FieldType: "uint64", ColumnName: "Anniversary", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "tags",
			typ:  "Tagged",
			expected: fields.Field{
				Children: []fields.Field{
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{ParquetType: "StringType", TypeName: "string", FieldName: "Name", FieldType: "string", ColumnName: "name", Category: "string", RepetitionType: fields.Required},
				},
			},
		},
		{
			name: "omit tag",
			typ:  "IgnoreMe",
			expected: fields.Field{
				Children: []fields.Field{
					{ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", FieldType: "int32", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
				},
			},
		},
		{
			name: "repeated",
			typ:  "Slice",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "IDs", ColumnName: "ids", Category: "numericOptional", RepetitionType: fields.Repeated},
				},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice2",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "IDs", ColumnName: "ids", Category: "numericOptional", RepetitionType: fields.Repeated},
				},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice3",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "IDs", ColumnName: "ids", Category: "numericOptional", RepetitionType: fields.Repeated},
					{ParquetType: "Int32Type", TypeName: "*int32", FieldName: "Age", FieldType: "int32", ColumnName: "Age", Category: "numericOptional", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "nested and repeated",
			typ:  "Slice4",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{Type: "Hobby", TypeName: "Hobby", FieldName: "Hobbies", FieldType: "Hobby", ColumnName: "hobbies", RepetitionType: fields.Repeated, Children: []fields.Field{
						{ParquetType: "StringType", TypeName: "string", FieldName: "Name", FieldType: "string", ColumnName: "Name", Category: "string", RepetitionType: fields.Required},
						{ParquetType: "Int32Type", TypeName: "int32", FieldName: "Difficulty", FieldType: "int32", ColumnName: "Difficulty", Category: "numeric", RepetitionType: fields.Required},
					}},
				},
			},
		},
		{
			name: "nested and repeated v2",
			typ:  "Slice5",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{Type: "Hobby2", TypeName: "Hobby2", FieldName: "Hobby", FieldType: "Hobby2", ColumnName: "hobby", RepetitionType: fields.Required, Children: []fields.Field{
						{ParquetType: "StringType", TypeName: "string", FieldName: "Names", FieldType: "string", ColumnName: "names", Category: "stringOptional", RepetitionType: fields.Repeated},
					}},
				},
			},
		},
		{
			name: "repeated and repeated",
			typ:  "Slice6",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
					{Type: "Hobby2", TypeName: "Hobby2", FieldName: "Hobbies", FieldType: "Hobby2", ColumnName: "hobbies", RepetitionType: fields.Repeated, Children: []fields.Field{
						{ParquetType: "StringType", TypeName: "string", FieldName: "Names", FieldType: "string", ColumnName: "names", Category: "stringOptional", RepetitionType: fields.Repeated},
					}},
				},
			},
		},
		{
			name: "nested repeated and repeated",
			typ:  "Slice7",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Slice6", TypeName: "*Slice6", FieldName: "Thing", FieldType: "Slice6", ColumnName: "thing", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldType: "int32", ParquetType: "Int32Type", TypeName: "int32", FieldName: "ID", ColumnName: "id", Category: "numeric", RepetitionType: fields.Required},
						{Type: "Hobby2", TypeName: "Hobby2", FieldName: "Hobbies", FieldType: "Hobby2", ColumnName: "hobbies", RepetitionType: fields.Repeated, Children: []fields.Field{
							{ParquetType: "StringType", TypeName: "string", FieldName: "Names", FieldType: "string", ColumnName: "names", Category: "stringOptional", RepetitionType: fields.Repeated},
						}},
					}},
				},
			},
		},
		{
			name: "dremel paper example",
			typ:  "Document",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldType: "int64", ParquetType: "Int64Type", TypeName: "int64", FieldName: "DocID", ColumnName: "DocID", Category: "numeric", RepetitionType: fields.Required},
					{Type: "Link", TypeName: "Link", FieldName: "Links", FieldType: "Link", ColumnName: "Links", RepetitionType: fields.Repeated, Children: []fields.Field{
						{TypeName: "int64", ParquetType: "Int64Type", FieldName: "Backward", FieldType: "int64", ColumnName: "Backward", Category: "numericOptional", RepetitionType: fields.Repeated},
						{TypeName: "int64", ParquetType: "Int64Type", FieldName: "Forward", FieldType: "int64", ColumnName: "Forward", Category: "numericOptional", RepetitionType: fields.Repeated},
					}},
					{Type: "Name", TypeName: "Name", FieldName: "Names", FieldType: "Name", ColumnName: "Names", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "Language", TypeName: "Language", FieldName: "Languages", FieldType: "Language", ColumnName: "Languages", RepetitionType: fields.Repeated, Children: []fields.Field{
							{TypeName: "string", ParquetType: "StringType", FieldName: "Code", FieldType: "string", ColumnName: "Code", Category: "string", RepetitionType: fields.Required},
							{TypeName: "*string", ParquetType: "StringType", FieldName: "Country", FieldType: "string", ColumnName: "Country", Category: "stringOptional", RepetitionType: fields.Optional},
						}},
						{TypeName: "*string", ParquetType: "StringType", FieldName: "URL", FieldType: "string", ColumnName: "URL", Category: "stringOptional", RepetitionType: fields.Optional},
					}},
				},
			},
		},
		{
			name: "embedded embedded embedded",
			typ:  "A",
			expected: fields.Field{
				Children: []fields.Field{
					{FieldName: "D", FieldType: "int32", TypeName: "int32", ParquetType: "Int32Type", ColumnName: "D", Category: "numeric", RepetitionType: fields.Required},
					{FieldName: "C", FieldType: "string", TypeName: "string", ParquetType: "StringType", ColumnName: "C", Category: "string", RepetitionType: fields.Required},
					{FieldName: "B", FieldType: "bool", TypeName: "bool", ParquetType: "BoolType", ColumnName: "B", Category: "bool", RepetitionType: fields.Required},
					{FieldName: "Name", FieldType: "string", TypeName: "string", ParquetType: "StringType", ColumnName: "Name", Category: "string", RepetitionType: fields.Required},
				},
			},
		},
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

func TestDefIndex(t *testing.T) {
	testCases := []struct {
		def      int
		field    fields.Field
		expected int
	}{
		{
			def: 1,
			field: fields.Field{
				RepetitionType: fields.Repeated,
				Parent: &fields.Field{
					RepetitionType: fields.Optional,
					Parent: &fields.Field{
						RepetitionType: fields.Required,
					},
				},
			},
			expected: 1,
		},
		{
			def: 2,
			field: fields.Field{
				RepetitionType: fields.Repeated,
				Parent: &fields.Field{
					RepetitionType: fields.Optional,
					Parent: &fields.Field{
						RepetitionType: fields.Required,
					},
				},
			},
			expected: 2,
		},
		{
			def: 0,
			field: fields.Field{
				RepetitionType: fields.Repeated,
				Parent: &fields.Field{
					RepetitionType: fields.Required,
					Parent: &fields.Field{
						RepetitionType: fields.Optional,
					},
				},
			},
			expected: 0,
		},
		{
			def: 2,
			field: fields.Field{
				RepetitionType: fields.Optional,
				Parent: &fields.Field{
					RepetitionType: fields.Required,
					Parent: &fields.Field{
						RepetitionType: fields.Repeated,
					},
				},
			},
			expected: 2,
		},
		{
			def: 2,
			field: fields.Field{
				RepetitionType: fields.Required,
				Parent: &fields.Field{
					RepetitionType: fields.Optional,
					Parent: &fields.Field{
						RepetitionType: fields.Repeated,
					},
				},
			},
			expected: 1,
		},
		{
			def: 1,
			field: fields.Field{
				RepetitionType: fields.Required,
				Parent: &fields.Field{
					RepetitionType: fields.Optional,
					Parent: &fields.Field{
						RepetitionType: fields.Repeated,
					},
				},
			},
			expected: 0,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.field.DefIndex(tc.def))
		})
	}
}
