package dremel_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/dremel"
	"github.com/parsyl/parquet/internal/fields"
	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	testCases := []struct {
		structName string
		name       string
		field      fields.Field
		result     string
	}{
		{
			name: "required and not nested",
			field: fields.Field{
				FieldType: "int32", TypeName: "int32", FieldName: "ID", RepetitionType: fields.Required,
			},
			result: `func writeID(x *Person, vals []int32) {
	x.ID = vals[0]
}`,
		},
		{
			name: "optional and not nested",
			field: fields.Field{
				FieldType: "int32", TypeName: "*int32", FieldName: "ID", RepetitionType: fields.Optional,
			},
			result: `func writeID(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.ID = pint32(vals[0])
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "required and nested",
			field: fields.Field{
				FieldName: "Other", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
						{FieldType: "int32", TypeName: "int32", FieldName: "Difficulty", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func writeOtherHobbyDifficulty(x *Person, vals []int32) {
	x.Other.Hobby.Difficulty = vals[0]
}`,
		},
		{
			name: "optional and nested",
			field: fields.Field{
				FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "int32", TypeName: "int32", FieldName: "Difficulty", RepetitionType: fields.Optional},
				},
			},
			result: `func writeHobbyDifficulty(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Hobby = &Hobby{}
	case 2:
		x.Hobby = &Hobby{Difficulty: pint32(vals[0])}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "optional and nested and seen by an optional fields",
			field: fields.Field{
				FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "string", TypeName: "string", FieldName: "Name", RepetitionType: fields.Required},
					{FieldType: "int32", TypeName: "int32", FieldName: "Difficulty", RepetitionType: fields.Optional},
				},
			},
			result: `func writeHobbyDifficulty(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 2:
		x.Hobby.Difficulty = pint32(vals[0])
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "mix of optional and required and nested",
			field: fields.Field{
				FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "string", TypeName: "string", FieldName: "Name", RepetitionType: fields.Required},
				},
			},
			result: `func writeHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Hobby = &Hobby{Name: vals[0]}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "mix of optional and required and nested v2",
			field: fields.Field{
				FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldType: "string", TypeName: "*string", FieldName: "Name", RepetitionType: fields.Optional},
				},
			},
			result: `func writeHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Hobby.Name = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Required, Children: []fields.Field{
						{FieldName: "Name", FieldType: "string", TypeName: "*string", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: Item{Name: pstring(vals[0])}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "mix of optional and required and nested 3 deep v2 and seen by optional field",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldType: "int", TypeName: "*int", FieldName: "Rank", RepetitionType: fields.Optional},
						{FieldType: "string", TypeName: "*string", FieldName: "Name", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 2:
		x.Friend.Hobby.Name = pstring(vals[0])
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "mix of optional and required and nested 3 deep v3",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldType: "string", TypeName: "*string", FieldName: "Name", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{Name: vals[0]}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "nested 3 deep all optional",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldType: "string", TypeName: "*string", FieldName: "Name", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{}}
	case 3:
		x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[0])}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "nested 3 deep all optional and seen by optional field",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "int", TypeName: "*int", FieldName: "Rank", RepetitionType: fields.Optional},
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldType: "string", TypeName: "*string", FieldName: "Name", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 2:
		x.Friend.Hobby = &Item{}
	case 3:
		x.Friend.Hobby = &Item{Name: pstring(vals[0])}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "four deep",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldType: "string", TypeName: "*string", FieldName: "First", RepetitionType: fields.Optional},
						}},
					}},
				},
			},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{}}
	case 3:
		x.Friend = &Entity{Hobby: &Item{Name: &Name{}}}
	case 4:
		x.Friend = &Entity{Hobby: &Item{Name: &Name{First: pstring(vals[0])}}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "four deep and seen by optional field",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "int", TypeName: "*int", FieldName: "Rank", RepetitionType: fields.Optional},
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldType: "string", TypeName: "*string", FieldName: "First", RepetitionType: fields.Optional},
						}},
					}},
				},
			},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 2:
		x.Friend.Hobby = &Item{}
	case 3:
		x.Friend.Hobby = &Item{Name: &Name{}}
	case 4:
		x.Friend.Hobby = &Item{Name: &Name{First: pstring(vals[0])}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "four deep mixed",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldType: "string", TypeName: "*string", FieldName: "First", RepetitionType: fields.Optional},
						}},
					}},
				},
			},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend.Hobby = &Item{}
	case 2:
		x.Friend.Hobby = &Item{Name: &Name{}}
	case 3:
		x.Friend.Hobby = &Item{Name: &Name{First: pstring(vals[0])}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "four deep mixed and seen by a required sub-field",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Required, Children: []fields.Field{
					{FieldType: "int", TypeName: "*int", FieldName: "Rank", RepetitionType: fields.Required},
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldType: "string", TypeName: "*string", FieldName: "First", RepetitionType: fields.Optional},
						}},
					}},
				},
			},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend.Hobby = &Item{}
	case 2:
		x.Friend.Hobby = &Item{Name: &Name{}}
	case 3:
		x.Friend.Hobby = &Item{Name: &Name{First: pstring(vals[0])}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "four deep mixed v2",
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldType: "string", TypeName: "*string", FieldName: "First", RepetitionType: fields.Required},
						}},
					}},
				},
			},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{}}
	case 3:
		x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[0]}}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name: "four deep mixed v2 and seen by an optional field",
			// fields: []fields.Field{
			// 	{FieldNames: []string{"Friend", "Rank"}, FieldTypes: []string{"Entity", "int"}, RepetitionTypes: []fields.RepetitionType{fields.Optional}},
			// 	{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
			// },
			field: fields.Field{
				FieldName: "Friend", FieldType: "Entity", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "int", TypeName: "*int", FieldName: "Rank", RepetitionType: fields.Optional},
					{FieldName: "Hobby", FieldType: "Item", RepetitionType: fields.Optional, Children: []fields.Field{
						{FieldName: "Name", FieldType: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{FieldType: "string", TypeName: "string", FieldName: "First", RepetitionType: fields.Required},
						}},
					}},
				},
			},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 2:
		x.Friend.Hobby = &Item{}
	case 3:
		x.Friend.Hobby = &Item{Name: &Name{First: vals[0]}}
		return 1, 1
	}

	return 0, 1
}`,
		},
		{
			name:       "writeLinkBackward",
			structName: "Document",
			field: fields.Field{
				FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "string", TypeName: "string", FieldName: "Backward", RepetitionType: fields.Repeated},
				},
			},
			result: `func writeLinkBackward(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			x.Link = &Link{}
		case 2:
			switch rep {
			case 0:
				x.Link = &Link{Backward: []string{vals[nVals]}}
			case 1:
				x.Link.Backward = append(x.Link.Backward, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:       "writeLinkFoward",
			structName: "Document",
			field: fields.Field{
				FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "string", TypeName: "string", FieldName: "Backward", RepetitionType: fields.Repeated},
					{FieldType: "string", TypeName: "string", FieldName: "Forward", RepetitionType: fields.Repeated},
				},
			},
			result: `func writeLinkForward(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			case 0:
				x.Link.Forward = []string{vals[nVals]}
			case 1:
				x.Link.Forward = append(x.Link.Forward, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:       "writeNamesLanguagesCode",
			structName: "Document",
			field: fields.Field{
				FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldType: "Language", FieldName: "Languages", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldType: "string", TypeName: "string", FieldName: "Code", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func writeNamesLanguagesCode(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 2)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			x.Names = append(x.Names, Name{})
		case 2:
			switch rep {
			case 0:
				x.Names = []Name{{Languages: []Language{{Code: vals[nVals]}}}}
			case 1:
				x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})
			case 2:
				x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name: "writeNamesLanguagesCountry",
			// fields: []fields.Field{
			// 	{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
			// 	{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
			// },
			structName: "Document",
			field: fields.Field{
				FieldName: "Names", FieldType: "Name", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldType: "Language", FieldName: "Languages", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldType: "string", TypeName: "string", FieldName: "Code", RepetitionType: fields.Required},
						{FieldType: "string", TypeName: "*string", FieldName: "Country", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func writeNamesLanguagesCountry(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 2)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 3:
			switch rep {
			case 0, 2:
				x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:       "writeFriendsID",
			structName: "Person",
			field: fields.Field{
				FieldName: "Friends", FieldType: "Being", RepetitionType: fields.Repeated, Children: []fields.Field{
					{FieldType: "int32", TypeName: "int32", FieldName: "ID", RepetitionType: fields.Required},
				},
			},
			result: `func writeFriendsID(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			switch rep {
			case 0:
				x.Friends = []Being{{ID: vals[nVals]}}
			case 1:
				x.Friends = append(x.Friends, Being{ID: vals[nVals]})
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:       "repeated primitive",
			structName: "Document",
			field: fields.Field{
				FieldName: "LuckyNumbers", TypeName: "int64", FieldType: "int64", RepetitionType: fields.Repeated,
			},
			result: `func writeLuckyNumbers(x *Document, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			switch rep {
			case 0:
				x.LuckyNumbers = []int64{vals[nVals]}
			case 1:
				x.LuckyNumbers = append(x.LuckyNumbers, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:       "repeated field not handled by previous repeated field",
			structName: "Document",
			field: fields.Field{
				FieldName: "Link", FieldType: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldType: "string", TypeName: "string", FieldName: "ID", RepetitionType: fields.Required},
					{FieldType: "string", TypeName: "string", FieldName: "Forward", RepetitionType: fields.Repeated},
				},
			},
			result: `func writeLinkForward(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			case 0:
				x.Link.Forward = []string{vals[nVals]}
			case 1:
				x.Link.Forward = append(x.Link.Forward, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:       "nested 2 deep",
			structName: "Person",
			field: fields.Field{
				FieldName: "Hobby", FieldType: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{FieldName: "Skills", FieldType: "Skill", RepetitionType: fields.Repeated, Children: []fields.Field{
						{FieldType: "string", TypeName: "string", FieldName: "Name", RepetitionType: fields.Required},
						{FieldType: "string", TypeName: "string", FieldName: "Difficulty", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func writeHobbySkillsDifficulty(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			switch rep {
			case 0, 1:
				x.Hobby.Skills[ind[0]].Difficulty = vals[nVals]
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			ty := tc.structName
			if ty == "" {
				ty = "Person"
			}
			flds := fields.Field{Type: ty, Children: []fields.Field{tc.field}}.Fields()
			f := flds[len(flds)-1]
			s := dremel.Write(f)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
