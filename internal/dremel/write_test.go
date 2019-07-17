package dremel_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/dremel"
	"github.com/parsyl/parquet/internal/parse"
	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	testCases := []struct {
		name   string
		seen   []parse.Field
		f      parse.Field
		result string
	}{
		{
			name: "required and not nested",
			f:    parse.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"ID"}, RepetitionTypes: []parse.RepetitionType{parse.Required}},
			result: `func writeID(x *Person, vals []int32) {
	x.ID = vals[0]
}`,
		},
		{
			name: "optional and not nested",
			f:    parse.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			result: `func writeID(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		v := vals[0]
		x.ID = &v
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "required and nested",
			f:    parse.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"Other", "Hobby", "Difficulty"}, FieldTypes: []string{"Other", "Hobby", "int32"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required, parse.Required}},
			result: `func writeOtherHobbyDifficulty(x *Person, vals []int32) {
	x.Other.Hobby.Difficulty = vals[0]
}`,
		},
		{
			name: "optional and nested",
			f:    parse.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional}},
			result: `func writeHobbyDifficulty(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Hobby == nil {
			x.Hobby = &Hobby{}
		}
	case 2:
		v := vals[0]
		if x.Hobby == nil {
			x.Hobby = &Hobby{Difficulty: &v}
		} else {
			x.Hobby.Difficulty = &v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "mix of optional and required and nested",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required}},
			result: `func writeHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		v := vals[0]
		if x.Hobby == nil {
			x.Hobby = &Hobby{Name: v}
		} else {
			x.Hobby.Name = v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "mix of optional and required and nested v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional}},
			result: `func writeHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		v := vals[0]
		x.Hobby.Name = &v
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required, parse.Optional}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend == nil {
			x.Friend = &Entity{}
		}
	case 2:
		v := vals[0]
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: Item{Name: &v}}
		} else {
			x.Friend.Hobby.Name = &v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional, parse.Optional}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{}
		}
	case 2:
		v := vals[0]
		if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &v}
		} else {
			x.Friend.Hobby.Name = &v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v3",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Required}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend == nil {
			x.Friend = &Entity{}
		}
	case 2:
		v := vals[0]
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{Name: v}}
		} else {
			x.Friend.Hobby.Name = v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "nested 3 deep all optional",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend == nil {
			x.Friend = &Entity{}
		}
	case 2:
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{}
		}
	case 3:
		v := vals[0]
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{Name: &v}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &v}
		} else {
			x.Friend.Hobby.Name = &v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "four deep",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional, parse.Optional}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend == nil {
			x.Friend = &Entity{}
		}
	case 2:
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{}
		}
	case 3:
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{Name: &Name{}}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &Name{}}
		} else if x.Friend.Hobby.Name == nil {
			x.Friend.Hobby.Name = &Name{}
		}
	case 4:
		v := vals[0]
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{Name: &Name{First: &v}}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &Name{First: &v}}
		} else if x.Friend.Hobby.Name == nil {
			x.Friend.Hobby.Name = &Name{First: &v}
		} else {
			x.Friend.Hobby.Name.First = &v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "four deep mixed",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional, parse.Optional, parse.Optional}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{}
		}
	case 2:
		if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &Name{}}
		} else if x.Friend.Hobby.Name == nil {
			x.Friend.Hobby.Name = &Name{}
		}
	case 3:
		v := vals[0]
		if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &Name{First: &v}}
		} else if x.Friend.Hobby.Name == nil {
			x.Friend.Hobby.Name = &Name{First: &v}
		} else {
			x.Friend.Hobby.Name.First = &v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "four deep mixed v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional, parse.Required}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def {
	case 1:
		if x.Friend == nil {
			x.Friend = &Entity{}
		}
	case 2:
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{}
		}
	case 3:
		v := vals[0]
		if x.Friend == nil {
			x.Friend = &Entity{Hobby: &Item{Name: &Name{First: v}}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &Name{First: v}}
		} else {
			x.Friend.Hobby.Name.First = v
		}
		return 1, 1
	}
	return 0, 1
}`,
		},
		{
			name: "writeLinkBackward",
			f:    parse.Field{Type: "Document", TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Repeated}},
			result: `func writeLinkBackward(x *Document, vals []int64, defs, reps []uint8) (int, int) {
	l := findLevel(reps[1:], 0) + 1
	defs = defs[:l]
	reps = reps[:l]

	var v int
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		switch def {
		case 1:
			x.Link = &Link{}
		case 2:
			if x.Link == nil {
				x.Link = &Link{}
			}
			switch rep {
			case 0, 1:
				x.Link.Backward = append(x.Link.Backward, vals[v])
				v++
			}
		}
	}

	return v, l
}`,
		},
		{
			name: "writeLinkFoward",
			f:    parse.Field{Type: "Document", TypeName: "int64", FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Repeated}},
			seen: []parse.Field{
				{FieldNames: []string{"Link", "Backward"}},
			},
			result: `func writeLinkForward(x *Document, vals []int64, defs, reps []uint8) (int, int) {
	l := findLevel(reps[1:], 0) + 1
	defs = defs[:l]
	reps = reps[:l]

	var v int
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		switch def {
		case 2:
			switch rep {
			case 0, 1:
				x.Link.Forward = append(x.Link.Forward, vals[v])
				v++
			}
		}
	}

	return v, l
}`,
		},
		{
			name: "writeNamesLanguagesCode",
			f:    parse.Field{Type: "Document", TypeName: "int64", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated, parse.Repeated, parse.Required}},
			result: `func writeNamesLanguagesCode(x *Document, vals []string, defs, reps []uint8) (int, int) {
	l := findLevel(reps[1:], 0) + 1
	defs = defs[:l]
	reps = reps[:l]

	var v int
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		switch def {
		case 2:
			switch rep {
			case 0, 1:
				x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[v]}}})
			case 2:
				x.Names[len(x.Names)-1].Languages = append(x.Names[len(x.Names)-1].Languages, Language{Code: vals[v]})
			}
			v++
		case 1:
			x.Names = append(x.Names, Name{})
		}
	}

	return v, l
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := dremel.Write(0, tc.f, tc.seen)
			gocode, err := format.Source([]byte(s))
			fmt.Println(string(gocode))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
