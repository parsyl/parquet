package fields_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/fields"
	"github.com/stretchr/testify/assert"
)

func TestNilFields(t *testing.T) {
	type testInput struct {
		f        fields.Field
		expected []string
	}

	testCases := []testInput{
		{
			f: fields.Field{FieldName: "First", RepetitionType: fields.Optional, Parent: &fields.Field{
				FieldName: "Name", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Friends", RepetitionType: fields.Repeated}}},
			expected: []string{
				"Friends",
				"Friends.Name.First",
			},
		},
		{
			f: fields.Field{FieldName: "First", RepetitionType: fields.Optional, Parent: &fields.Field{FieldName: "Name", RepetitionType: fields.Required, Parent: &fields.Field{FieldName: "Friend", RepetitionType: fields.Required}}},
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

func TestInit(t *testing.T) {
	testCases := []struct {
		fields   []fields.Field
		def      int
		rep      int
		expected string
	}{
		{
			fields: []fields.Field{
				{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Backward", FieldType: "int64", RepetitionType: fields.Repeated},
				}},
			},
			rep:      0,
			def:      1,
			expected: "x.Links = &Link{}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Backward", FieldType: "int64", RepetitionType: fields.Repeated},
				}},
			},
			rep:      0,
			def:      2,
			expected: "x.Links = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Backward", FieldType: "int64", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Links.Backward = append(x.Links.Backward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Links", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Forward", FieldType: "int64", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Links.Forward = append(x.Links.Forward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Code", FieldType: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			fields: []fields.Field{
				{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Backward", FieldType: "string", RepetitionType: fields.Repeated},
				}},
			},
			def:      1,
			rep:      0,
			expected: "x.Link = &Link{}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Backward", FieldType: "string", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []string{vals[nVals]}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Backward", FieldType: "string", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Language", FieldType: "Language", RepetitionType: fields.Required, Children: []fields.Field{
						{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = []Language{{Codes: []string{vals[nVals]}}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Language", FieldType: "Language", RepetitionType: fields.Required, Children: []fields.Field{
						{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Language.Codes = append(x.Names[ind[0]].Language.Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Name.Languages[ind[0]].Codes = append(x.Name.Languages[ind[0]].Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Thing", FieldType: "Thing", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
							{FieldName: "Codes", FieldType: "string", RepetitionType: fields.Repeated},
						}},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Thing.Names[ind[0]].Languages[ind[1]].Codes = append(x.Thing.Names[ind[0]].Languages[ind[1]].Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Required, Children: []fields.Field{
						{FieldName: "Name", FieldType: "string", TypeName: "*string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: Item{Name: pstring(vals[0])}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "string", TypeName: "*string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "int", TypeName: "*int", FieldName: "Rank", RepetitionType: fields.Optional},
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "string", TypeName: "*string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Difficulty", FieldType: "int32", RepetitionType: fields.Optional},
				}},
			},
			def:      1,
			expected: "x.Hobby = &Hobby{}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Difficulty", FieldType: "int32", RepetitionType: fields.Optional},
				}},
			},
			def:      2,
			expected: "x.Hobby = &Hobby{Difficulty: pint32(vals[0])}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional},
					{FieldName: "Difficulty", FieldType: "int32", RepetitionType: fields.Optional},
				}},
			},
			def:      2,
			expected: "x.Hobby.Difficulty = pint32(vals[0])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Name", FieldType: "string", RepetitionType: fields.Required},
				}},
			},
			def:      1,
			expected: "x.Hobby = &Hobby{Name: vals[0]}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional},
				}},
			},
			def:      1,
			expected: "x.Hobby.Name = pstring(vals[0])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional},
				}},
			},
			def:      1,
			expected: "x.Hobby = &Item{}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional},
				}},
			},
			def:      2,
			expected: "x.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      1,
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Country", FieldType: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			fields: []fields.Field{
				{FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldName: "Languages", FieldType: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldName: "Zip", FieldType: "string", RepetitionType: fields.Optional},
						{FieldName: "Country", FieldType: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			rep:      0,
			expected: "x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldName: "First", FieldType: "string", RepetitionType: fields.Required},
						}},
					}},
				}},
			},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldName: "First", FieldType: "string", RepetitionType: fields.Required},
						}},
					}},
				}},
			},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldName: "First", FieldType: "string", RepetitionType: fields.Required},
						}},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[0]}}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldName: "Suffix", FieldType: "string", RepetitionType: fields.Optional},
							{FieldName: "First", FieldType: "string", RepetitionType: fields.Optional},
						}},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend.Hobby.Name.First = pstring(vals[0])",
		},
		{
			fields: []fields.Field{
				{FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Name", FieldType: "string", RepetitionType: fields.Repeated},
					{FieldName: "Forward", FieldType: "int64", RepetitionType: fields.Repeated},
				}},
			},
			rep:      1,
			def:      2,
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "LuckyNumbers", FieldType: "int64", RepetitionType: fields.Repeated},
			},
			def:      1,
			rep:      0,
			expected: "x.LuckyNumbers = []int64{vals[nVals]}",
		},
		{
			fields: []fields.Field{
				{FieldName: "LuckyNumbers", FieldType: "int64", RepetitionType: fields.Repeated},
			},
			def:      1,
			rep:      1,
			expected: "x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{FieldName: "A", FieldType: "A", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "B", FieldType: "B", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "C", FieldType: "C", RepetitionType: fields.Required, Children: []fields.Field{
							{FieldName: "D", FieldType: "D", RepetitionType: fields.Repeated, Children: []fields.Field{
								{FieldName: "E", FieldType: "E", RepetitionType: fields.Required, Children: []fields.Field{
									{FieldName: "F", FieldType: "string", RepetitionType: fields.Optional},
								}},
							}},
						}},
					}},
				}},
			},
			def:      3,
			rep:      0,
			expected: "x.A.B = &B{C: C{D: []D{{E: E{F: pstring(vals[nVals])}}}}}",
		},
		{
			fields: []fields.Field{
				{FieldName: "A", FieldType: "A", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "B", FieldType: "B", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "C", FieldType: "C", RepetitionType: fields.Required, Children: []fields.Field{
							{FieldName: "D", FieldType: "D", RepetitionType: fields.Repeated, Children: []fields.Field{
								{FieldName: "E", FieldType: "E", RepetitionType: fields.Required, Children: []fields.Field{
									{FieldName: "x", FieldType: "string", RepetitionType: fields.Optional},
									{FieldName: "F", FieldType: "string", RepetitionType: fields.Optional},
								}},
							}},
						}},
					}},
				}},
			},
			def:      3,
			expected: "x.A.B.C.D[ind[0]].E.F = pstring(vals[nVals])",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d def %d rep %d", i, tc.def, tc.rep), func(t *testing.T) {
			fields := fields.Field{Children: tc.fields}.Fields()
			field := fields[len(fields)-1]
			s := field.Init(tc.def, tc.rep)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, string(gocode))
		})
	}
}
