package structs_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/parse"
	"github.com/parsyl/parquet/internal/structs"
	sch "github.com/parsyl/parquet/schema"
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
			field:    parse.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional}},
			def:      1,
			expected: "&Item{}",
		},
		{
			name:     "2 deep def 2 of 2",
			field:    parse.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional}},
			def:      2,
			expected: "&Item{Name: &v}",
		},
		{
			name:     "3 deep def 1 of 2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required, parse.Optional}},
			def:      1,
			expected: "&Entity{}",
		},
		{
			name:     "3 deep def 2 of 2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required, parse.Optional}},
			def:      2,
			expected: "&Entity{Hobby: Item{Name: &v}}",
		},
		{
			name:     "3 deep def 1 of 2 v2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Required}},
			def:      1,
			expected: "&Entity{}",
		},
		{
			name:     "3 deep def 2 of 2 v2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Required}},
			def:      2,
			expected: "&Entity{Hobby: &Item{Name: v}}",
		},
		{
			name:     "3 deep def 1 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			def:      1,
			expected: "&Entity{}",
		},
		{
			name:     "3 deep def 2 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			def:      2,
			expected: "&Entity{Hobby: &Item{}}",
		},
		{
			name:     "3 deep def 3 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			def:      3,
			expected: "&Entity{Hobby: &Item{Name: &v}}",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.expected, structs.Init(tc.def, tc.field))
		})
	}
}

func TestStruct(t *testing.T) {
	type testInput struct {
		name     string
		schema   []*sch.SchemaElement
		expected string
	}

	testCases := []testInput{
		{
			name: "single field",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(1)},
				{Name: "id", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
			},
			expected: `type Root struct {
	Id int32
}`,
		},
		{
			name: "single nested field",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(1)},
				{Name: "hobby", RepetitionType: prt(sch.FieldRepetitionType_REQUIRED), NumChildren: pint32(1)},
				{Name: "name", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
			},
			expected: `type Root struct {
	Hobby Hobby
}

type Hobby struct {
	Name *string
}`,
		},
		{
			name: "nested 3 deep",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(2)},
				{Name: "hobby", RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL), NumChildren: pint32(2)},
				{Name: "name", RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL), NumChildren: pint32(2)},
				{Name: "first", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
				{Name: "last", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
				{Name: "difficulty", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
				{Name: "id", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
			},
			expected: `type Root struct {
	Hobby *Hobby
	Id    int32
}

type Hobby struct {
	Name       *Name
	Difficulty *int32
}

type Name struct {
	First *string
	Last  string
}`,
		},
		{
			name: "nested 3 deep v2",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(2)},
				{Name: "hobby", RepetitionType: prt(sch.FieldRepetitionType_REQUIRED), NumChildren: pint32(2)},
				{Name: "name", RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL), NumChildren: pint32(2)},
				{Name: "first", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
				{Name: "last", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
				{Name: "difficulty", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
				{Name: "id", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
			},
			expected: `type Root struct {
	Hobby Hobby
	Id    *int32
}

type Hobby struct {
	Name       *Name
	Difficulty int32
}

type Name struct {
	First *string
	Last  string
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := structs.Struct("Root", tc.schema)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			if !assert.Equal(t, tc.expected, string(gocode)) {
				fmt.Println(string(gocode))
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
