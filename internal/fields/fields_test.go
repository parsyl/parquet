package fields_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/fields"
	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {
	testCases := []struct {
		i        int
		field    fields.Field
		def      int
		rep      int
		seen     []fields.RepetitionType
		expected string
	}{
		{
			i:        1,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      1,
			expected: "x.Link = &Link{}",
		},
		{
			i:        2,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			i:        3,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Optional},
			expected: "x.Link = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			i:        4,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			i:        5,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			i:        6,
			field:    fields.Field{TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			i:        7,
			field:    fields.Field{TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			i:        8,
			field:    fields.Field{TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			i:        9,
			field:    fields.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Hobby{}",
		},
		{
			i:        10,
			field:    fields.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      2,
			expected: "x.Hobby = &Hobby{Difficulty: pint32(vals[0])}",
		},
		{
			i:        11,
			field:    fields.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Hobby.Difficulty = pint32(vals[0])",
		},
		{
			i:        12,
			field:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
			def:      1,
			expected: "x.Hobby = &Hobby{Name: vals[0]}",
		},
		{
			i:        13,
			field:    fields.Field{TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional}},
			def:      1,
			expected: "x.Hobby.Name = pstring(vals[0])",
		},
		{
			i:        14,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			i:        15,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			i:        16,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			i:        17,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			i:        18,
			field:    fields.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      1,
			rep:      0,
			expected: "x.Link = &Link{}",
		},
		{
			i:        19,
			field:    fields.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			i:        20,
			field:    fields.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			i:        21,
			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{
			i:        22,
			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Language: Language{Codes: []string{vals[nVals]}}})",
		},
		{
			i:        23,
			field:    fields.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Name.Languages = append(x.Name.Languages, Language{Codes: []string{vals[nVals]}})",
		},
		{
			i:        24,
			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{
			i:        25,
			field:    fields.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = []Language{{Codes: []string{vals[nVals]}}}",
		},
		{
			i:        26,
			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Language.Codes = append(x.Names[ind[0]].Language.Codes, vals[nVals])",
		},
		{
			i:        27,
			field:    fields.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated}},
			def:      2,
			rep:      2,
			expected: "x.Name.Languages[ind[0]].Codes = append(x.Name.Languages[ind[0]].Codes, vals[nVals])",
		},
		{
			i:        28,
			field:    fields.Field{FieldNames: []string{"Thing", "Names", "Languages", "Codes"}, FieldTypes: []string{"Thing", "Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated, fields.Repeated}},
			def:      3,
			rep:      3,
			expected: "x.Thing.Names[ind[0]].Languages[ind[1]].Codes = append(x.Thing.Names[ind[0]].Languages[ind[1]].Codes, vals[nVals])",
		},
		{
			i:        29,
			field:    fields.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Item{}",
		},
		{
			i:        30,
			field:    fields.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      2,
			expected: "x.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			i:        31,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			i:        32,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional}},
			def:      1,
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			i:        33,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			i:        34,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			def:      3,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
			expected: "x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])",
		},
		{
			i:        35,
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			def:      3,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Names[ind[0]].Languages = []Language{{Country: pstring(vals[nVals])}}",
		},
		{
			i:        36,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			i:        37,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Optional},
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			i:        38,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			i:        39,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			i:        40,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[0]}}}",
		},
		{
			i:        41,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Friend.Hobby = &Item{Name: &Name{First: vals[0]}}",
		},
		{
			i:        42,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
			expected: "x.Friend.Hobby.Name = &Name{First: vals[0]}",
		},
		{
			i:        43,
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Repeated},
			expected: "x.Friend.Hobby.Name.First = vals[0]",
		},
		{
			i:        44,
			field:    fields.Field{TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional, fields.Optional}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Repeated},
			expected: "x.Friend.Hobby.Name.First = pstring(vals[0])",
		},
		{
			i:        45,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Optional},
			expected: "x.Link = &Link{Forward: []int64{vals[nVals]}}",
		},
		{
			i:        46,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"LuckyNumbers"}, FieldTypes: []string{"int64"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
			def:      1,
			rep:      0,
			expected: "x.LuckyNumbers = []int64{vals[nVals]}",
		},
		{
			i:        47,
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"LuckyNumbers"}, FieldTypes: []string{"int64"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
			def:      1,
			rep:      1,
			expected: "x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])",
		},
		{
			i:        48,
			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%02d", tc.i), func(t *testing.T) {
			field := tc.field
			field.Seen = tc.seen
			s := field.Init(tc.def, tc.rep)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, string(gocode))
		})
	}
}

func TestSeen(t *testing.T) {
	testCases := []struct {
		flds     []fields.Field
		expected []fields.RepetitionType
	}{
		{
			flds: []fields.Field{
				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			},
			expected: []fields.RepetitionType{fields.Optional},
		},
		{
			flds: []fields.Field{
				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Required}},
				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated}},
			},
			expected: []fields.RepetitionType{fields.Required},
		},
		{
			flds: []fields.Field{
				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
				{FieldNames: []string{"Link", "Backward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			},
			expected: []fields.RepetitionType{fields.Repeated},
		},
		{
			flds: []fields.Field{
				{FieldNames: []string{"Name", "First"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			},
			expected: []fields.RepetitionType{},
		},
		{
			flds: []fields.Field{
				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
				{FieldNames: []string{"Link", "Name", "First"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated, fields.Optional}},
				{FieldNames: []string{"Link", "Name", "Last"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated, fields.Required}},
			},
			expected: []fields.RepetitionType{fields.Repeated, fields.Repeated},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			i := len(tc.flds) - 1
			assert.Equal(t, tc.expected, fields.Seen(i, tc.flds))
		})
	}
}

func TestChild(t *testing.T) {
	f := fields.Field{
		FieldNames:      []string{"Friends", "Name", "First"},
		FieldTypes:      []string{"Being", "Name", "string"},
		RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Optional},
	}
	ch := fields.Field{
		FieldNames:      []string{"Name", "First"},
		FieldTypes:      []string{"Name", "string"},
		RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional},
	}
	assert.Equal(t, ch, f.Child(1))
}
