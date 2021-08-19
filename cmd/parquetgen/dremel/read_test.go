package dremel_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/cmd/parquetgen/dremel"
	"github.com/parsyl/parquet/cmd/parquetgen/fields"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	testCases := []struct {
		name       string
		structName string
		f          fields.Field
		result     string
	}{
		{
			name: "required and not nested",
			f: fields.Field{
				Type: "int32", Name: "ID", RepetitionType: fields.Required,
			},
			result: `func readID(x Person) int32 {
	return x.ID
}`,
		},
		{
			name: "optional and not nested",
			////f:    fields.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional}},
			f: fields.Field{
				Type: "int32", Name: "ID", RepetitionType: fields.Optional,
			},
			result: `func readID(x Person, vals []int32, defs, reps []uint8) ([]int32, []uint8, []uint8) {
	switch {
	case x.ID == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	default:
		vals = append(vals, *x.ID)
		defs = append(defs, 1)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "required and nested",
			f: fields.Field{
				Name: "Other", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "int32", Name: "Difficulty", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func readOtherHobbyDifficulty(x Person) int32 {
	return x.Other.Hobby.Difficulty
}`,
		},
		{
			name: "optional and nested",
			f: fields.Field{
				Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{Type: "int32", Name: "Difficulty", RepetitionType: fields.Optional},
				},
			},
			result: `func readHobbyDifficulty(x Person, vals []int32, defs, reps []uint8) ([]int32, []uint8, []uint8) {
	switch {
	case x.Hobby == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Hobby.Difficulty == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Hobby.Difficulty)
		defs = append(defs, 2)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "mix of optional and required and nested",
			f: fields.Field{
				Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
					{Type: "string", Name: "Name", RepetitionType: fields.Required},
				},
			},
			result: `func readHobbyName(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Hobby == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	default:
		vals = append(vals, x.Hobby.Name)
		defs = append(defs, 1)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "mix of optional and required and nested v2",
			f: fields.Field{
				Name: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
					{Type: "string", Name: "Name", RepetitionType: fields.Optional},
				},
			},
			result: `func readHobbyName(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Hobby.Name == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Hobby.Name)
		defs = append(defs, 1)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "string", Name: "Name", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func readFriendHobbyName(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby.Name == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Friend.Hobby.Name)
		defs = append(defs, 2)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v2",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Type: "string", Name: "Name", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func readFriendHobbyName(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend.Hobby == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby.Name == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Friend.Hobby.Name)
		defs = append(defs, 2)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v3",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Type: "string", Name: "Name", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func readFriendHobbyName(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	default:
		vals = append(vals, x.Friend.Hobby.Name)
		defs = append(defs, 2)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "nested 3 deep all optional",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Type: "string", Name: "Name", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func readFriendHobbyName(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	case x.Friend.Hobby.Name == nil:
		defs = append(defs, 2)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Friend.Hobby.Name)
		defs = append(defs, 3)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "four deep",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Type: "string", Name: "First", RepetitionType: fields.Optional},
						}},
					}},
				},
			},
			result: `func readFriendHobbyNameFirst(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	case x.Friend.Hobby.Name == nil:
		defs = append(defs, 2)
		return vals, defs, reps
	case x.Friend.Hobby.Name.First == nil:
		defs = append(defs, 3)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Friend.Hobby.Name.First)
		defs = append(defs, 4)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "four deep mixed",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Type: "string", Name: "First", RepetitionType: fields.Optional},
						}},
					}},
				},
			},
			result: `func readFriendHobbyNameFirst(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend.Hobby == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby.Name == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	case x.Friend.Hobby.Name.First == nil:
		defs = append(defs, 2)
		return vals, defs, reps
	default:
		vals = append(vals, *x.Friend.Hobby.Name.First)
		defs = append(defs, 3)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "four deep mixed v2",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Hobby", RepetitionType: fields.Optional, Children: []fields.Field{
						{Name: "Name", RepetitionType: fields.Optional, Children: []fields.Field{
							{Type: "string", Name: "First", RepetitionType: fields.Required},
						}},
					}},
				},
			},
			result: `func readFriendHobbyNameFirst(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		defs = append(defs, 0)
		return vals, defs, reps
	case x.Friend.Hobby == nil:
		defs = append(defs, 1)
		return vals, defs, reps
	case x.Friend.Hobby.Name == nil:
		defs = append(defs, 2)
		return vals, defs, reps
	default:
		vals = append(vals, x.Friend.Hobby.Name.First)
		defs = append(defs, 3)
		return vals, defs, reps
	}
}`,
		},
		{
			name: "repeated",
			f: fields.Field{
				Type: "string", Name: "Friends", RepetitionType: fields.Repeated,
			},
			result: `func readFriends(x Person, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if len(x.Friends) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Friends {
			if i0 >= 1 {
				lastRep = 1
			}
			defs = append(defs, 1)
			reps = append(reps, lastRep)
			vals = append(vals, x0)
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "readLinkFoward",
			structName: "Document",
			f: fields.Field{
				Name: "Link", RepetitionType: fields.Optional, Children: []fields.Field{
					{Type: "int64", Name: "Forward", RepetitionType: fields.Repeated},
				},
			},
			result: `func readLinkForward(x Document, vals []int64, defs, reps []uint8) ([]int64, []uint8, []uint8) {
	var lastRep uint8

	if x.Link == nil {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		if len(x.Link.Forward) == 0 {
			defs = append(defs, 1)
			reps = append(reps, lastRep)
		} else {
			for i0, x0 := range x.Link.Forward {
				if i0 >= 1 {
					lastRep = 1
				}
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, x0)
			}
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "readNamesLanguagesCode",
			structName: "Document",
			f: fields.Field{
				Name: "Names", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "string", Name: "Code", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func readNamesLanguagesCode(x Document, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if len(x.Names) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			if i0 >= 1 {
				lastRep = 1
			}
			if len(x0.Languages) == 0 {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				for i1, x1 := range x0.Languages {
					if i1 >= 1 {
						lastRep = 2
					}
					defs = append(defs, 2)
					reps = append(reps, lastRep)
					vals = append(vals, x1.Code)
				}
			}
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "readNamesLanguagesCountry",
			structName: "Document",
			f: fields.Field{
				Name: "Names", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Languages", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Type: "string", Name: "Country", RepetitionType: fields.Optional},
					}},
				},
			},
			result: `func readNamesLanguagesCountry(x Document, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if len(x.Names) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			if i0 >= 1 {
				lastRep = 1
			}
			if len(x0.Languages) == 0 {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				for i1, x1 := range x0.Languages {
					if i1 >= 1 {
						lastRep = 2
					}
					if x1.Country == nil {
						defs = append(defs, 2)
						reps = append(reps, lastRep)
					} else {
						defs = append(defs, 3)
						reps = append(reps, lastRep)
						vals = append(vals, *x1.Country)
					}
				}
			}
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "readNamesURL",
			structName: "Document",
			f: fields.Field{
				Name: "Names", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Type: "string", Name: "URL", RepetitionType: fields.Optional},
				},
			},
			result: `func readNamesURL(x Document, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if len(x.Names) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			if i0 >= 1 {
				lastRep = 1
			}
			if x0.URL == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.URL)
			}
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "run of required",
			structName: "Document",
			f: fields.Field{
				Name: "Friends", RepetitionType: fields.Repeated, Children: []fields.Field{
					{Name: "Name", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "string", Name: "Last", RepetitionType: fields.Required},
					}},
				},
			},
			result: `func readFriendsNameLast(x Document, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if len(x.Friends) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Friends {
			if i0 >= 1 {
				lastRep = 1
			}
			defs = append(defs, 1)
			reps = append(reps, lastRep)
			vals = append(vals, x0.Name.Last)
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "run of required v2",
			structName: "Document",
			f: fields.Field{
				Name: "Friend", RepetitionType: fields.Required, Children: []fields.Field{
					{Name: "Name", RepetitionType: fields.Required, Children: []fields.Field{
						{Type: "string", Name: "Aliases", RepetitionType: fields.Repeated},
					}},
				},
			},
			result: `func readFriendNameAliases(x Document, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if len(x.Friend.Name.Aliases) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Friend.Name.Aliases {
			if i0 >= 1 {
				lastRep = 1
			}
			defs = append(defs, 1)
			reps = append(reps, lastRep)
			vals = append(vals, x0)
		}
	}

	return vals, defs, reps
}`,
		},
		{
			name:       "run of required v3",
			structName: "Document",
			f: fields.Field{
				Name: "Other", RepetitionType: fields.Optional, Children: []fields.Field{
					{Name: "Friends", RepetitionType: fields.Repeated, Children: []fields.Field{
						{Name: "Name", RepetitionType: fields.Required, Children: []fields.Field{
							{Type: "string", Name: "Middle", RepetitionType: fields.Required},
						}},
					}},
				},
			},
			result: `func readOtherFriendsNameMiddle(x Document, vals []string, defs, reps []uint8) ([]string, []uint8, []uint8) {
	var lastRep uint8

	if x.Other == nil {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		if len(x.Other.Friends) == 0 {
			defs = append(defs, 1)
			reps = append(reps, lastRep)
		} else {
			for i0, x0 := range x.Other.Friends {
				if i0 >= 1 {
					lastRep = 1
				}
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, x0.Name.Middle)
			}
		}
	}

	return vals, defs, reps
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			sn := tc.structName
			if sn == "" {
				sn = "Person"
			}
			flds := fields.Field{Type: sn, Children: []fields.Field{tc.f}}.Fields()
			s := dremel.Read(flds[len(flds)-1])
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
