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

func TestAppend(t *testing.T) {
	testCases := []struct {
		name     string
		field    parse.Field
		def      int
		rep      int
		expected string
	}{
		{
			name:     "nested required",
			field:    parse.Field{FieldNames: []string{"Name", "Language", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required, parse.Required}},
			def:      0,
			rep:      0,
			expected: "x.Name = Name{Language: Language{Code: v}}",
		},
		{
			name:     "NamesLanguagesCode, def 1, rep 1",
			field:    parse.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Repeated, parse.Required}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			name:     "NamesLanguagesCode, def 2, rep 1",
			field:    parse.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Repeated, parse.Required}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[v]}}})",
		},
		{
			name:     "NamesLanguagesCode, def 2, rep 0",
			field:    parse.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Repeated, parse.Required}},
			def:      2,
			rep:      0,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[v]}}})",
		},
		{
			name:     "NamesLanguagesCode, def 2, rep 2",
			field:    parse.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Repeated, parse.Required}},
			def:      2,
			rep:      2,
			expected: "x.Names[len(x.Names)-1].Languages = append(x.Names[len(x.Names)-1].Languages, Language{Code: vals[v]})",
		},
		{
			name:     "LinkBackward, def 2, rep 0",
			field:    parse.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[v])",
		},
		{
			name:     "LinkBackward, def 2, rep 1",
			field:    parse.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[v])",
		},
		{
			name:     "repeated required repeated",
			field:    parse.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Required, parse.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Names = append(x.Names, Name{Language: Language{Codes: []string{vals[v]}}})",
		},
		{
			name:     "required repeated repeated",
			field:    parse.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Repeated, parse.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = append(x.Name.Languages, Language{Codes: []string{vals[v]}})",
		},
		{
			name:     "repeated required repeated rep 1",
			field:    parse.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Required, parse.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Language: Language{Codes: []string{vals[v]}}})",
		},
		{
			name:     "required repeated repeated rep 1",
			field:    parse.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Repeated, parse.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Name.Languages = append(x.Name.Languages, Language{Codes: []string{vals[v]}})",
		},
		{
			name:     "repeated required repeated rep 0",
			field:    parse.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Required, parse.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Names = append(x.Names, Name{Language: Language{Codes: []string{vals[v]}}})",
		},
		{
			name:     "required repeated repeated rep 0",
			field:    parse.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Repeated, parse.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = append(x.Name.Languages, Language{Codes: []string{vals[v]}})",
		},
		{
			name:     "repeated required repeated rep 2",
			field:    parse.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Required, parse.Repeated}},
			def:      2,
			rep:      2,
			expected: "x.Names[len(x.Names)-1].Language.Codes = append(x.Names[len(x.Names)-1].Language.Codes, vals[v])",
		},
		{
			name:     "required repeated repeated rep 2",
			field:    parse.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Repeated, parse.Repeated}},
			def:      2,
			rep:      2,
			expected: "x.Name.Languages[len(x.Name.Languages)-1].Codes = append(x.Name.Languages[len(x.Name.Languages)-1].Codes, vals[v])",
		},
		{
			name:     "required repeated repeated repeated rep 3",
			field:    parse.Field{FieldNames: []string{"Thing", "Names", "Languages", "Codes"}, FieldTypes: []string{"Thing", "Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Repeated, parse.Repeated, parse.Repeated}},
			def:      3,
			rep:      3,
			expected: "x.Thing.Names[len(x.Thing.Names)-1].Languages[len(x.Thing.Names[len(x.Thing.Names)-1].Languages)-1].Codes = append(x.Thing.Names[len(x.Thing.Names)-1].Languages[len(x.Thing.Names[len(x.Thing.Names)-1].Languages)-1].Codes, vals[v])",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.expected, structs.Init(tc.def, tc.rep, tc.field))
		})
	}
}

/*
| names    | Friend | Hobby | Name   | definition levels      |                                 |
| types    | Entity | Item  | string | 1/2                    | 2/2                             |
|----------+--------+-------+--------+------------------------+---------------------------------|
| optional | true   | false | true   | &Entity{}              | &Entity{Hobby: Item{Name: v}}   |
|          | false  | true  | true   | Entity{Hobby: &Item{}} | &Entity{Hobby: Item{Name: v}}   |
|          | true   | true  | false  | &Entity{}              | &Entity{Hobby: &Item{Name: *v}} |
*/

func TestInit(t *testing.T) {
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
			expected: "x.Hobby = &Item{}",
		},
		{
			name:     "2 deep def 2 of 2",
			field:    parse.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional}},
			def:      2,
			expected: "x.Hobby = &Item{Name: &v}",
		},
		{
			name:     "3 deep def 1 of 2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required, parse.Optional}},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			name:     "3 deep def 2 of 2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required, parse.Optional}},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: Item{Name: &v}}",
		},
		{
			name:     "3 deep def 1 of 2 v2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Required}},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			name:     "3 deep def 2 of 2 v2",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Required}},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: v}}",
		},
		{
			name:     "3 deep def 1 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			name:     "3 deep def 2 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			name:     "3 deep def 3 of 3",
			field:    parse.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: &v}}",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.expected, structs.Init(tc.def, 0, tc.field))
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
