package structs_test

import (
	"fmt"
	"testing"

	"github.com/parsyl/parquet/internal/parse"
	"github.com/parsyl/parquet/internal/structs"
	"github.com/stretchr/testify/assert"
)

/*
| names    | Friend | Hobby | Name   | definition levels      |                                 |
| types    | Entity | Item  | string | 1/2                    | 2/2                             |
|----------+--------+-------+--------+------------------------+---------------------------------|
| optional | true   | false | true   | &Entity{}              | &Entity{Hobby: Item{Name: v}}   |
|          | false  | true  | true   | Entity{Hobby: &Item{}} | &Entity{Hobby: Item{Name: v}}   |
|          | true   | true  | false  | &Entity{}              | &Entity{Hobby: &Item{Name: *v}} |
*/

func TestStructs(t *testing.T) {
	testCases := []struct {
		name     string
		field    parse.Field
		def      int
		expected string
	}{
		{
			name:     "2 deep def 1 of 2",
			field:    parse.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []bool{true, true}},
			def:      1,
			expected: "&Item{}",
		},
		{
			name:     "2 deep def 2 of 2",
			field:    parse.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []bool{true, true}},
			def:      2,
			expected: "&Item{Name: v}",
		},
		{
			name:     "3 deep def 1 of 2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, false, true}},
			def:      1,
			expected: "&Entity{}",
		},
		{
			name:     "3 deep def 2 of 2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, false, true}},
			def:      2,
			expected: "&Entity{Hobby: Item{Name: v}}",
		},
		{
			name:     "3 deep def 1 of 2 v2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, true, false}},
			def:      1,
			expected: "&Entity{}",
		},
		{
			name:     "3 deep def 2 of 2 v2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, true, false}},
			def:      2,
			expected: "&Entity{Hobby: &Item{Name: *v}}",
		},
		{
			name:     "3 deep def 1 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, true, true}},
			def:      1,
			expected: "&Entity{}",
		},
		{
			name:     "3 deep def 2 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, true, true}},
			def:      2,
			expected: "&Entity{Hobby: &Item{}}",
		},
		{
			name:     "3 deep def 3 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []bool{true, true, true}},
			def:      3,
			expected: "&Entity{Hobby: &Item{Name: v}}",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.expected, structs.Init(tc.def, tc.field))
		})
	}
}
