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
					{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "private fields",
			typ:  "Private",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "nested struct",
			typ:  "Nested",
			expected: fields.Field{
				Children: []fields.Field{
					{Name: "Being", Type: "Being", ColumnName: "Being", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
						{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
					}},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name: "nested struct with name that doesn't match the struct type",
			typ:  "Nested2",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Being", Name: "Info", ColumnName: "Info", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
						{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
					}},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name: "2 deep nested struct",
			typ:  "DoubleNested",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Nested", Name: "Nested", ColumnName: "Nested", Children: []fields.Field{
						{Type: "Being", Name: "Being", ColumnName: "Being", RepetitionType: fields.Required, Children: []fields.Field{
							{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
							{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
						}},
						{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
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
					{Type: "OptionalNested", Name: "OptionalNested", ColumnName: "OptionalNested", Children: []fields.Field{
						{Type: "Being", Name: "Being", ColumnName: "Being", RepetitionType: fields.Optional, Children: []fields.Field{
							{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
							{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
						}},
						{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
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
					{Type: "Being", Name: "Being", ColumnName: "Being", RepetitionType: fields.Optional, Children: []fields.Field{
						{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
						{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
					}},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
				},
			},
			errors: []error{},
		},
		{
			name: "optional nested struct v2",
			typ:  "OptionalNested2",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Thing", Name: "Being", ColumnName: "Being", RepetitionType: fields.Optional, Children: []fields.Field{
						{Type: "string", Name: "Name", ColumnName: "Name", RepetitionType: fields.Required},
					}},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
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
					{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "unsupported fields mixed in with supported and embedded",
			typ:  "SupportedAndUnsupported",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int64", Name: "Happiness", ColumnName: "Happiness", RepetitionType: fields.Required},
					{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
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
					{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
					{Type: "int64", Name: "Happiness", ColumnName: "Happiness", RepetitionType: fields.Required},
					{Type: "int64", Name: "Sadness", ColumnName: "Sadness", RepetitionType: fields.Optional},
					{Type: "string", Name: "Code", ColumnName: "Code", RepetitionType: fields.Required},
					{Type: "float32", Name: "Funkiness", ColumnName: "Funkiness", RepetitionType: fields.Required},
					{Type: "float32", Name: "Lameness", ColumnName: "Lameness", RepetitionType: fields.Optional},
					{Type: "bool", Name: "Keen", ColumnName: "Keen", RepetitionType: fields.Optional},
					{Type: "uint32", Name: "Birthday", ColumnName: "Birthday", RepetitionType: fields.Required},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "embedded preserve order",
			typ:  "NewOrderPerson",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int64", Name: "Happiness", ColumnName: "Happiness", RepetitionType: fields.Required},
					{Type: "int64", Name: "Sadness", ColumnName: "Sadness", RepetitionType: fields.Optional},
					{Type: "string", Name: "Code", ColumnName: "Code", RepetitionType: fields.Required},
					{Type: "float32", Name: "Funkiness", ColumnName: "Funkiness", RepetitionType: fields.Required},
					{Type: "float32", Name: "Lameness", ColumnName: "Lameness", RepetitionType: fields.Optional},
					{Type: "bool", Name: "Keen", ColumnName: "Keen", RepetitionType: fields.Optional},
					{Type: "uint32", Name: "Birthday", ColumnName: "Birthday", RepetitionType: fields.Required},
					{Type: "int32", Name: "ID", ColumnName: "ID", RepetitionType: fields.Required},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
					{Type: "uint64", Name: "Anniversary", ColumnName: "Anniversary", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "tags",
			typ:  "Tagged",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
					{Type: "string", Name: "Name", ColumnName: "name", RepetitionType: fields.Required},
				},
			},
		},
		{
			name: "omit tag",
			typ:  "IgnoreMe",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
				},
			},
		},
		{
			name: "repeated",
			typ:  "Slice",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "IDs", ColumnName: "ids", RepetitionType: fields.Repeated},
				},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice2",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
					{Type: "int32", Name: "IDs", ColumnName: "ids", RepetitionType: fields.Repeated},
				},
			},
		},
		{
			name: "repeated v2",
			typ:  "Slice3",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
					{Type: "int32", Name: "IDs", ColumnName: "ids", RepetitionType: fields.Repeated},
					{Type: "int32", Name: "Age", ColumnName: "Age", RepetitionType: fields.Optional},
				},
			},
		},
		{
			name: "nested and repeated",
			typ:  "Slice4",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
					{Type: "Hobby", Name: "Hobbies", ColumnName: "hobbies", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "string", Name: "Name", ColumnName: "Name", RepetitionType: fields.Required},
						{Type: "int32", Name: "Difficulty", ColumnName: "Difficulty", RepetitionType: fields.Required},
					}},
				},
			},
		},
		{
			name: "nested and repeated v2",
			typ:  "Slice5",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
					{Type: "Hobby2", Name: "Hobby", ColumnName: "hobby", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "string", Name: "Names", ColumnName: "names", RepetitionType: fields.Repeated},
					}},
				},
			},
		},
		{
			name: "repeated and repeated",
			typ:  "Slice6",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
					{Type: "Hobby2", Name: "Hobbies", ColumnName: "hobbies", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "string", Name: "Names", ColumnName: "names", RepetitionType: fields.Repeated},
					}},
				},
			},
		},
		{
			name: "nested repeated and repeated",
			typ:  "Slice7",
			expected: fields.Field{
				Children: []fields.Field{
					{Type: "Slice6", Name: "Thing", ColumnName: "thing", RepetitionType: fields.Optional, Children: []fields.Field{
						{Type: "int32", Name: "ID", ColumnName: "id", RepetitionType: fields.Required},
						{Type: "Hobby2", Name: "Hobbies", ColumnName: "hobbies", RepetitionType: fields.Repeated, Children: []fields.Field{
							{Type: "string", Name: "Names", ColumnName: "names", RepetitionType: fields.Repeated},
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
					{Type: "int64", Name: "DocID", ColumnName: "DocID", RepetitionType: fields.Required},
					{Type: "Link", Name: "Links", ColumnName: "Links", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "int64", Name: "Backward", ColumnName: "Backward", RepetitionType: fields.Repeated},
						{Type: "int64", Name: "Forward", ColumnName: "Forward", RepetitionType: fields.Repeated},
					}},
					{Type: "Name", Name: "Names", ColumnName: "Names", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "Language", Name: "Languages", ColumnName: "Languages", RepetitionType: fields.Repeated, Children: []fields.Field{
							{Type: "string", Name: "Code", ColumnName: "Code", RepetitionType: fields.Required},
							{Type: "string", Name: "Country", ColumnName: "Country", RepetitionType: fields.Optional},
						}},
						{Type: "string", Name: "URL", ColumnName: "URL", RepetitionType: fields.Optional},
					}},
				},
			},
		},
		{
			name: "embedded embedded embedded",
			typ:  "A",
			expected: fields.Field{
				Children: []fields.Field{
					{Name: "D", Type: "int32", ColumnName: "D", RepetitionType: fields.Required},
					{Name: "C", Type: "string", ColumnName: "C", RepetitionType: fields.Required},
					{Name: "B", Type: "bool", ColumnName: "B", RepetitionType: fields.Required},
					{Name: "Name", Type: "string", ColumnName: "Name", RepetitionType: fields.Required},
				},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			out, err := parse.Fields(tc.typ, "./parse_test.go")
			assert.Nil(t, err, tc.name)

			if len(tc.errors) == 0 {
				tc.errors = nil
			}

			if !assert.Equal(t, tc.errors, out.Errors, tc.name) {
				return
			}

			assert.Equal(t, tc.expected.Children, out.Parent.Children, tc.name)
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
