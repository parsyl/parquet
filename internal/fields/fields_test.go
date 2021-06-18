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
			f: fields.Field{Name: "First", RepetitionType: fields.Optional, Parent: &fields.Field{
				Name: "Name", RepetitionType: fields.Required, Parent: &fields.Field{Name: "Friends", RepetitionType: fields.Repeated}}},
			expected: []string{
				"Friends",
				"Friends.Name.First",
			},
		},
		{
			f: fields.Field{Name: "First", RepetitionType: fields.Optional, Parent: &fields.Field{Name: "Name", RepetitionType: fields.Required, Parent: &fields.Field{Name: "Friend", RepetitionType: fields.Required}}},
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

			f := fields.Field{Type: "Person", Children: []fields.Field{tc.f}}

			for i := 0; i < f.MaxDef(); i++ {
				s, _, _, _ := f.NilField(i)
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
				{Name: "Links", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Backward", Type: "int64", RepetitionType: fields.Repeated},
				}},
			},
			rep:      0,
			def:      1,
			expected: "x.Links = &Link{}",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Backward", Type: "int64", RepetitionType: fields.Repeated},
				}},
			},
			rep:      0,
			def:      2,
			expected: "x.Links = &Link{Backward: []int64{vals[nVals]}}",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Backward", Type: "int64", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Links.Backward = append(x.Links.Backward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Forward", Type: "int64", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Links.Forward = append(x.Links.Forward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Code", Type: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Code", Type: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Code", Type: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Code", Type: "int64", RepetitionType: fields.Required},
					}},
				}},
			},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			fields: []fields.Field{
				{Name: "Link", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Backward", Type: "string", RepetitionType: fields.Repeated},
				}},
			},
			def:      1,
			rep:      0,
			expected: "x.Link = &Link{}",
		},
		{
			fields: []fields.Field{
				{Name: "Link", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Backward", Type: "string", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Link = &Link{Backward: []string{vals[nVals]}}",
		},
		{
			fields: []fields.Field{
				{Name: "Link", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Backward", Type: "string", RepetitionType: fields.Repeated},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Link.Backward = append(x.Link.Backward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Language", Type: "Language", RepetitionType: fields.Required, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Names = []Name{{Language: Language{Codes: []string{vals[nVals]}}}}",
		},
		{
			fields: []fields.Field{
				{Name: "Name", Type: "Name", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Name.Languages = []Language{{Codes: []string{vals[nVals]}}}",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Language", Type: "Language", RepetitionType: fields.Required, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Names[ind[0]].Language.Codes = append(x.Names[ind[0]].Language.Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Name", Type: "Name", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Name.Languages[ind[0]].Codes = append(x.Name.Languages[ind[0]].Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Thing", Type: "Thing", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
							{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
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
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Required, Children: []fields.Field{
						{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: Item{Name: pstring(vals[0])}}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Type: "int", Name: "Rank", RepetitionType: fields.Optional},
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Difficulty", Type: "int32", RepetitionType: fields.Optional},
				}},
			},
			def:      1,
			expected: "x.Hobby = &Hobby{}",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Difficulty", Type: "int32", RepetitionType: fields.Optional},
				}},
			},
			def:      2,
			expected: "x.Hobby = &Hobby{Difficulty: pint32(vals[0])}",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					{Name: "Difficulty", Type: "int32", RepetitionType: fields.Optional},
				}},
			},
			def:      2,
			expected: "x.Hobby.Difficulty = pint32(vals[0])",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Required},
				}},
			},
			def:      1,
			expected: "x.Hobby = &Hobby{Name: vals[0]}",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Optional},
				}},
			},
			def:      1,
			expected: "x.Hobby.Name = pstring(vals[0])",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Optional},
				}},
			},
			def:      1,
			expected: "x.Hobby = &Item{}",
		},
		{
			fields: []fields.Field{
				{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Optional},
				}},
			},
			def:      2,
			expected: "x.Hobby = &Item{Name: pstring(vals[0])}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      1,
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      2,
			expected: "x.Friend.Hobby = &Item{}",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Country", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      1,
			rep:      1,
			expected: "x.Names = append(x.Names, Name{})",
		},
		{
			fields: []fields.Field{
				{Name: "Names", Type: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Zip", Type: "string", RepetitionType: fields.Optional},
						{Name: "Country", Type: "string", RepetitionType: fields.Optional},
					}},
				}},
			},
			def:      3,
			rep:      0,
			expected: "x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Name: "First", Type: "string", RepetitionType: fields.Required},
						}},
					}},
				}},
			},
			def:      1,
			expected: "x.Friend = &Entity{}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Name: "First", Type: "string", RepetitionType: fields.Required},
						}},
					}},
				}},
			},
			def:      2,
			expected: "x.Friend = &Entity{Hobby: &Item{}}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Name: "First", Type: "string", RepetitionType: fields.Required},
						}},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[0]}}}",
		},
		{
			fields: []fields.Field{
				{Name: "Friend", Type: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", Type: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", Type: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Name: "Suffix", Type: "string", RepetitionType: fields.Optional},
							{Name: "First", Type: "string", RepetitionType: fields.Optional},
						}},
					}},
				}},
			},
			def:      3,
			expected: "x.Friend.Hobby.Name.First = pstring(vals[0])",
		},
		{
			fields: []fields.Field{
				{
					Name: "Hobby", Type: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Skills", Type: "Skill", RepetitionType: fields.Repeated, Children: []fields.Field{
							{Type: "string", Name: "Name", RepetitionType: fields.Required},
							{Type: "string", Name: "Difficulty", RepetitionType: fields.Required},
						}},
					},
				},
			},
			def:      2,
			rep:      1,
			expected: "x.Hobby.Skills[ind[0]].Difficulty = vals[nVals]",
		},
		{
			fields: []fields.Field{
				{Name: "Link", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Repeated},
					{Name: "Forward", Type: "int64", RepetitionType: fields.Repeated},
				}},
			},
			rep:      1,
			def:      2,
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Link", Type: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Name", Type: "string", RepetitionType: fields.Repeated},
					{Name: "Forward", Type: "string", RepetitionType: fields.Repeated},
				}},
			},
			rep:      0,
			def:      2,
			expected: "x.Link.Forward = append(x.Link.Forward, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "LuckyNumbers", Type: "int64", RepetitionType: fields.Repeated},
			},
			def:      1,
			rep:      0,
			expected: "x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "LuckyNumbers", Type: "int64", RepetitionType: fields.Repeated},
			},
			def:      1,
			rep:      1,
			expected: "x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "A", Type: "A", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "B", Type: "B", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "C", Type: "C", RepetitionType: fields.Required, Children: []fields.Field{
							{Name: "D", Type: "D", RepetitionType: fields.Repeated, Children: []fields.Field{
								{Name: "E", Type: "E", RepetitionType: fields.Required, Children: []fields.Field{
									{Name: "F", Type: "string", RepetitionType: fields.Optional},
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
				{Name: "A", Type: "A", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "B", Type: "B", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "C", Type: "C", RepetitionType: fields.Required, Children: []fields.Field{
							{Name: "D", Type: "D", RepetitionType: fields.Repeated, Children: []fields.Field{
								{Name: "E", Type: "E", RepetitionType: fields.Required, Children: []fields.Field{
									{Name: "x", Type: "string", RepetitionType: fields.Optional},
									{Name: "F", Type: "string", RepetitionType: fields.Optional},
								}},
							}},
						}},
					}},
				}},
			},
			def:      3,
			expected: "x.A.B.C.D[ind[0]].E.F = pstring(vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      1,
			rep:      1,
			expected: "x.Links = append(x.Links, Link{})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Links[ind[0]].Backward = append(x.Links[ind[0]].Backward, Language{})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Links[ind[0]].Backward[ind[1]].Codes = append(x.Links[ind[0]].Backward[ind[1]].Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
						{Name: "Countries", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
						{Name: "Countries", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Links[ind[0]].Forward[ind[1]].Countries = append(x.Links[ind[0]].Forward[ind[1]].Countries, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
						{Name: "Countries", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      2,
			expected: "x.Links[ind[0]].Forward = append(x.Links[ind[0]].Forward, Language{Codes: []string{vals[nVals]}})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
						{Name: "Countries", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Links[ind[0]].Forward[ind[1]].Codes = append(x.Links[ind[0]].Forward[ind[1]].Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      1,
			rep:      0,
			expected: "x.Links = append(x.Links, Link{})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      1,
			rep:      1,
			expected: "x.Links = append(x.Links, Link{})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      0,
			expected: "x.Links = []Link{{Backward: []Language{{}}}}",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      1,
			expected: "x.Links = append(x.Links, Link{Backward: []Language{{}}})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Links[ind[0]].Backward = append(x.Links[ind[0]].Backward, Language{})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      1,
			expected: "x.Links = append(x.Links, Link{Backward: []Language{{Codes: []string{vals[nVals]}}}})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      2,
			expected: "x.Links[ind[0]].Backward = append(x.Links[ind[0]].Backward, Language{Codes: []string{vals[nVals]}})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Links[ind[0]].Backward[ind[1]].Codes = append(x.Links[ind[0]].Backward[ind[1]].Codes, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
						{Name: "Countries", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Links[ind[0]].Forward[ind[1]].Countries = append(x.Links[ind[0]].Forward[ind[1]].Countries, vals[nVals])",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      2,
			rep:      2,
			expected: "x.Links[ind[0]].Forward = append(x.Links[ind[0]].Forward, Language{})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      2, //1 isn't a valid rep because it is handled by Links.Backward.Codes
			expected: "x.Links[ind[0]].Forward = append(x.Links[ind[0]].Forward, Language{Codes: []string{vals[nVals]}})",
		},
		{
			fields: []fields.Field{
				{Name: "Links", Type: "Link", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Backward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
					{Name: "Forward", Type: "Language", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Codes", Type: "string", RepetitionType: fields.Repeated},
					}},
				}},
			},
			def:      3,
			rep:      3,
			expected: "x.Links[ind[0]].Forward[ind[1]].Codes = append(x.Links[ind[0]].Forward[ind[1]].Codes, vals[nVals])",
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
