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
		nthChild int
		expected string
	}{
		{
			field:    fields.Field{FieldName: "Backward", FieldType: "int64", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional}},
			rep:      0,
			def:      1,
			expected: "x.Links = &Link{}",
		},
		{
			field:    fields.Field{FieldName: "Backward", FieldType: "int64", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional}},
			rep:      0,
			def:      2,
			expected: "x.Links = &Link{Backward: []int64{vals[nVals]}}",
		},

		{
			field:    fields.Field{FieldName: "Backward", FieldType: "int64", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional}},
			def:      2,
			rep:      1,
			expected: "x.Links.Backward = append(x.Links.Backward, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "Forward", FieldType: "int64", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional}},
			def:      2,
			rep:      1,
			expected: "x.Links.Forward = append(x.Links.Forward, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			field:    fields.Field{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			field:    fields.Field{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			field:    fields.Field{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			field:    fields.Field{FieldName: "Backward", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional}},
			def:      1,
			rep:      0,
			expected: "x.Link = &Link{}",
		},
		{
			field:    fields.Field{FieldName: "Backward", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional}},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []string{vals[nVals]}}",
		},
		{
			field:    fields.Field{FieldName: "Backward", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional}},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Language", FieldType: "Language", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{
			field:    fields.Field{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Required}}},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = []Language{{Codes: []string{vals[nVals]}}}",
		},
		{
			field:    fields.Field{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Language", FieldType: "Language", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Language.Codes = append(x.Names[ind[0]].Language.Codes, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Required}}},
			def:      2,
			rep:      2,
			expected: "x.Name.Languages[ind[0]].Codes = append(x.Name.Languages[ind[0]].Codes, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Thing", FieldType: "Thing", RepetitionType: fields.Required}}}},
			def:      3,
			rep:      3,
			expected: "x.Thing.Names[ind[0]].Languages[ind[1]].Codes = append(x.Thing.Names[ind[0]].Languages[ind[1]].Codes, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "Difficulty", FieldType: "int32", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Hobby{}",
		},
		{
			field:    fields.Field{FieldName: "Difficulty", FieldType: "int32", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional}},
			def:      2,
			expected: "x.Hobby = &Hobby{Difficulty: pint32(vals[0])}",
		},
		{
			field:    fields.Field{FieldName: "Difficulty", FieldType: "int32", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional}},
			def:      2,
			nthChild: 1,
			expected: "x.Hobby.Difficulty = pint32(vals[0])",
		},
		{
			field:    fields.Field{FieldName: "Name", FieldType: "string", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Hobby{Name: vals[0]}",
		},
		{
			field:    fields.Field{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Required}},
			def:      1,
			expected: "x.Hobby.Name = pstring(vals[0])",
		},
		{
			field:    fields.Field{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional}},
			def:      1,
			expected: "x.Hobby = &Item{}",
		},
		{
			field:    fields.Field{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional}},
			def:      2,
			expected: "x.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			field:    fields.Field{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional}}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			field:    fields.Field{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Required}}},
			def:      1,
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			field:    fields.Field{FieldName: "Country", FieldType: "string", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			field:    fields.Field{FieldName: "Country", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated}}},
			def:      3,
			rep:      0,
			nthChild: 1,
			expected: "x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "First", FieldType: "string", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional}}}},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			field:    fields.Field{FieldName: "First", FieldType: "string", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional}}}},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			field:    fields.Field{FieldName: "First", FieldType: "string", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional}}}},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[0]}}}",
		},
		{
			field:    fields.Field{FieldName: "First", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional}}}},
			def:      3,
			nthChild: 1,
			expected: "x.Friend.Hobby.Name.First = pstring(vals[0])",
		},
		{
			field:    fields.Field{FieldName: "Forward", FieldType: "int64", RepetitionType: fields.Repeated, Parent: &fields.Field{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional}},
			rep:      1,
			def:      2,
			nthChild: 1,
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			field:    fields.Field{FieldName: "LuckyNumbers", FieldType: "int64", RepetitionType: fields.Repeated},
			def:      1,
			rep:      0,
			expected: "x.LuckyNumbers = []int64{vals[nVals]}",
		},
		{
			field:    fields.Field{FieldName: "LuckyNumbers", FieldType: "int64", RepetitionType: fields.Repeated},
			def:      1,
			rep:      1,
			expected: "x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])",
		},
		{
			field: fields.Field{FieldName: "F", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{
				FieldName: "E", FieldType: "E", RepetitionType: fields.Required, Parent: &fields.Field{
					FieldName: "D", FieldType: "D", RepetitionType: fields.Repeated, Parent: &fields.Field{
						FieldName: "C", FieldType: "C", RepetitionType: fields.Required, Parent: &fields.Field{
							FieldName: "B", FieldType: "B", RepetitionType: fields.Optional, Parent: &fields.Field{
								FieldName: "A", FieldType: "A", RepetitionType: fields.Required}}}}}},
			def:      3,
			rep:      0,
			expected: "x.A.B = &B{C: C{D: []D{{E: E{F: pstring(vals[nVals])}}}}}",
		},
		{
			field: fields.Field{FieldName: "F", FieldType: "string", RepetitionType: fields.Optional, Parent: &fields.Field{
				FieldName: "E", FieldType: "E", RepetitionType: fields.Required, Parent: &fields.Field{
					FieldName: "D", FieldType: "D", RepetitionType: fields.Repeated, Parent: &fields.Field{
						FieldName: "C", FieldType: "C", RepetitionType: fields.Required, Parent: &fields.Field{
							FieldName: "B", FieldType: "B", RepetitionType: fields.Optional, Parent: &fields.Field{
								FieldName: "A", FieldType: "A", RepetitionType: fields.Required}}}}}},
			def:      3,
			nthChild: 1,
			expected: "x.A.B.C.D[ind[0]].E.F = pstring(vals[nVals])",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %v def %d rep %d", i, tc.field.FieldNames(), tc.def, tc.rep), func(t *testing.T) {
			field := tc.field
			s := field.Init(tc.def, tc.rep, tc.nthChild)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, string(gocode))
		})
	}
}

// func TestSeen(t *testing.T) {
// 	testCases := []struct {
// 		flds     []fields.Field
// 		expected []fields.RepetitionType
// 	}{
// 		{
// 			flds: []fields.Field{
// 				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
// 				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
// 			},
// 			expected: []fields.RepetitionType{fields.Optional},
// 		},
// 		{
// 			flds: []fields.Field{
// 				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Required}},
// 				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Repeated}},
// 			},
// 			expected: []fields.RepetitionType{fields.Required},
// 		},
// 		{
// 			flds: []fields.Field{
// 				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
// 				{FieldNames: []string{"Link", "Backward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
// 				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
// 			},
// 			expected: []fields.RepetitionType{fields.Repeated},
// 		},
// 		{
// 			flds: []fields.Field{
// 				{FieldNames: []string{"Name", "First"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
// 				{FieldNames: []string{"Link", "Forward"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
// 			},
// 			expected: []fields.RepetitionType{},
// 		},
// 		{
// 			flds: []fields.Field{
// 				{FieldNames: []string{"Link", "ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
// 				{FieldNames: []string{"Link", "Name", "First"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated, fields.Optional}},
// 				{FieldNames: []string{"Link", "Name", "Last"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated, fields.Required}},
// 			},
// 			expected: []fields.RepetitionType{fields.Repeated, fields.Repeated},
// 		},
// 	}

// 	for i, tc := range testCases {
// 		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
// 			i := len(tc.flds) - 1
// 			assert.Equal(t, tc.expected, fields.Seen(i, tc.flds))
// 		})
// 	}
// }

// func TestChild(t *testing.T) {
// 	f := fields.Field{
// 		FieldNames:      []string{"Friends", "Name", "First"},
// 		FieldTypes:      []string{"Being", "Name", "string"},
// 		RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Optional},
// 	}
// 	ch := fields.Field{
// 		FieldNames:      []string{"Name", "First"},
// 		FieldTypes:      []string{"Name", "string"},
// 		RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional},
// 	}
// 	assert.Equal(t, ch, f.Child(1))
// }

// func TestRepCases(t *testing.T) {
// 	testCases := []struct {
// 		f        fields.Field
// 		seen     []fields.RepetitionType
// 		expected []fields.RepCase
// 	}{
// 		{
// 			f:        fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
// 			expected: []fields.RepCase{{Case: "case 0:", Rep: 0}, {Case: "case 1:", Rep: 1}, {Case: "case 2:", Rep: 2}},
// 		},
// 		{
// 			f:        fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
// 			seen:     []fields.RepetitionType{fields.Repeated, fields.Repeated},
// 			expected: []fields.RepCase{{Case: "default:", Rep: 0}},
// 		},
// 	}

// 	for i, tc := range testCases {
// 		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
// 			assert.Equal(t, tc.expected, tc.f.RepCases(tc.seen))
// 		})
// 	}
// }

// func TestNilField(t *testing.T) {
// 	f := fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}}
// 	name, rt, i, reps := f.NilField(1)
// 	assert.Equal(t, "Names.Languages", name)
// 	assert.Equal(t, fields.Repeated, rt)
// 	assert.Equal(t, 1, i)
// 	assert.Equal(t, 2, reps)
// }

// func TestField(t *testing.T) {
// 	f := fields.Field{FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}}
// 	assert.True(t, f.Repeated())
// 	assert.True(t, f.Optional())
// 	assert.False(t, f.Required())
// }

// func TestRepetitionTypes(t *testing.T) {
// 	rts := fields.RepetitionTypes([]fields.RepetitionType{fields.Repeated, fields.Optional})
// 	assert.Equal(t, rts.Def(1), fields.Repeated)
// 	assert.Equal(t, rts.Def(2), fields.Optional)
// }
