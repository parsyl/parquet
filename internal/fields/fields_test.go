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
		field    fields.Field
		def      int
		rep      int
		seen     []fields.RepetitionType
		expected string
	}{
		{
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      1,
			expected: "x.Link = &Link{}",
		},
		{
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			field:    fields.Field{TypeName: "int64", FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			field:    fields.Field{TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			field:    fields.Field{TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			field:    fields.Field{TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			field:    fields.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Hobby{}",
		},
		{
			field:    fields.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      2,
			expected: "x.Hobby = &Hobby{Difficulty: pint32(vals[0])}",
		},
		{
			field:    fields.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Hobby.Difficulty = pint32(vals[0])",
		},
		{
			field:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
			def:      1,
			expected: "x.Hobby = &Hobby{Name: vals[0]}",
		},
		{
			field:    fields.Field{TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional}},
			def:      1,
			expected: "x.Hobby.Name = pstring(vals[0])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			field:    fields.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      1,
			rep:      0,
			expected: "x.Link = &Link{}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{

			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{

			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Language: Language{Codes: []string{vals[nVals]}}})",
		},
		{

			field:    fields.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated}},
			def:      2,
			rep:      1,
			expected: "x.Name.Languages = append(x.Name.Languages, Language{Codes: []string{vals[nVals]}})",
		},
		{

			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{

			field:    fields.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated}},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = []Language{{Codes: []string{vals[nVals]}}}",
		},
		{

			field:    fields.Field{FieldNames: []string{"Names", "Language", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Repeated}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Language.Codes = append(x.Names[ind[0]].Language.Codes, vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Name", "Languages", "Codes"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated}},
			def:      2,
			rep:      2,
			expected: "x.Name.Languages[ind[0]].Codes = append(x.Name.Languages[ind[0]].Codes, vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Thing", "Names", "Languages", "Codes"}, FieldTypes: []string{"Thing", "Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated, fields.Repeated, fields.Repeated}},
			def:      3,
			rep:      3,
			expected: "x.Thing.Names[ind[0]].Languages[ind[1]].Codes = append(x.Thing.Names[ind[0]].Languages[ind[1]].Codes, vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Item{}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
			def:      2,
			expected: "x.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional}},
			def:      1,
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			def:      3,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
			expected: "x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			def:      3,
			rep:      0,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Names[ind[0]].Languages = []Language{{Country: pstring(vals[nVals])}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[0]}}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Friend.Hobby = &Item{Name: &Name{First: vals[0]}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
			expected: "x.Friend.Hobby.Name = &Name{First: vals[0]}",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Repeated},
			expected: "x.Friend.Hobby.Name.First = vals[0]",
		},
		{
			field:    fields.Field{FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional, fields.Optional}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Repeated},
			expected: "x.Friend.Hobby.Name.First = pstring(vals[0])",
		},
		{
			field:    fields.Field{FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			def:      2,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"LuckyNumbers"}, FieldTypes: []string{"int64"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
			def:      1,
			rep:      0,
			expected: "x.LuckyNumbers = []int64{vals[nVals]}",
		},
		{
			field:    fields.Field{FieldNames: []string{"LuckyNumbers"}, FieldTypes: []string{"int64"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
			def:      1,
			rep:      1,
			expected: "x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])",
		},
		{
			field:    fields.Field{FieldNames: []string{"A", "B", "C", "D", "E", "F"}, FieldTypes: []string{"A", "B", "C", "D", "E", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Required, fields.Repeated, fields.Required, fields.Optional}},
			def:      3,
			expected: "x.A.B = &B{C: C{D: []D{{E: E{F: pstring(vals[nVals])}}}}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"A", "B", "C", "D", "E", "F"}, FieldTypes: []string{"A", "B", "C", "D", "E", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Required, fields.Repeated, fields.Required, fields.Optional}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated},
			expected: "x.A.B = &B{C: C{D: []D{{E: E{F: pstring(vals[nVals])}}}}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"A", "B", "C", "D", "E", "F"}, FieldTypes: []string{"A", "B", "C", "D", "E", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Required, fields.Repeated, fields.Required, fields.Optional}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
			expected: "x.A.B.C.D = []D{{E: E{F: pstring(vals[nVals])}}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"A", "B", "C", "D", "E", "F"}, FieldTypes: []string{"A", "B", "C", "D", "E", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Required, fields.Repeated, fields.Required, fields.Optional}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Repeated},
			expected: "x.A.B.C.D = []D{{E: E{F: pstring(vals[nVals])}}}",
		},
		{
			field:    fields.Field{FieldNames: []string{"A", "B", "C", "D", "E", "F"}, FieldTypes: []string{"A", "B", "C", "D", "E", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Required, fields.Repeated, fields.Required, fields.Optional}},
			def:      3,
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Repeated, fields.Repeated},
			expected: "x.A.B.C.D[ind[0]].E.F = pstring(vals[nVals])",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %v def %d rep %d %v", i, tc.field.FieldNames, tc.def, tc.rep, tc.seen), func(t *testing.T) {
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

func TestRepCases(t *testing.T) {
	testCases := []struct {
		f        fields.Field
		seen     []fields.RepetitionType
		expected []fields.RepCase
	}{
		{
			f:        fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			expected: []fields.RepCase{{Case: "case 0:", Rep: 0}, {Case: "case 1:", Rep: 1}, {Case: "case 2:", Rep: 2}},
		},
		{
			f:        fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
			expected: []fields.RepCase{{Case: "default:", Rep: 0}},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.f.RepCases(tc.seen))
		})
	}
}

func TestNilField(t *testing.T) {
	f := fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}}
	name, rt, i, reps := f.NilField(1)
	assert.Equal(t, "Names.Languages", name)
	assert.Equal(t, fields.Repeated, rt)
	assert.Equal(t, 1, i)
	assert.Equal(t, 2, reps)
}

func TestField(t *testing.T) {
	f := fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}}
	assert.True(t, f.Repeated())
	assert.True(t, f.Optional())
	assert.False(t, f.Required())
}

func TestRepetitionTypes(t *testing.T) {
	rts := fields.RepetitionTypes([]fields.RepetitionType{fields.Repeated, fields.Optional})
	assert.Equal(t, rts.Def(1), fields.Repeated)
	assert.Equal(t, rts.Def(2), fields.Optional)
}
