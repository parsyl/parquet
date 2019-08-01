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
		name   string
		fields []fields.Field
		result string
	}{
		{
			name:   "required and not nested",
			fields: []fields.Field{{Type: "Person", TypeName: "int32", FieldNames: []string{"ID"}, RepetitionTypes: []fields.RepetitionType{fields.Required}}},
			result: `func writeID(x *Person, vals []int32) {
	x.ID = vals[0]
}`,
		},
		{
			name:   "optional and not nested",
			fields: []fields.Field{{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, FieldTypes: []string{"int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional}}},
			result: `func writeID(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.ID = pint32(vals[nVals])
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "required and nested",
			fields: []fields.Field{{Type: "Person", TypeName: "int32", FieldNames: []string{"Other", "Hobby", "Difficulty"}, FieldTypes: []string{"Other", "Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Required, fields.Required}}},
			result: `func writeOtherHobbyDifficulty(x *Person, vals []int32) {
	x.Other.Hobby.Difficulty = vals[0]
}`,
		},
		{
			name:   "optional and nested",
			fields: []fields.Field{{Type: "Person", TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}}},
			result: `func writeHobbyDifficulty(x *Person, vals []int32, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Hobby = &Hobby{}
	case 2:
		x.Hobby = &Hobby{Difficulty: pint32(vals[nVals])}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "mix of optional and required and nested",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}}},
			result: `func writeHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Hobby = &Hobby{Name: vals[nVals]}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "mix of optional and required and nested v2",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional}}},
			result: `func writeHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Hobby.Name = pstring(vals[nVals])
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "mix of optional and require and nested 3 deep",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required, fields.Optional}}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: Item{Name: pstring(vals[nVals])}}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "mix of optional and require and nested 3 deep v2",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional}}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend.Hobby = &Item{}
	case 2:
		x.Friend.Hobby = &Item{Name: pstring(vals[nVals])}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "mix of optional and require and nested 3 deep v3",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Required}}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{Name: vals[nVals]}}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "nested 3 deep all optional",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional}}},
			result: `func writeFriendHobbyName(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{}}
	case 3:
		x.Friend = &Entity{Hobby: &Item{Name: pstring(vals[nVals])}}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "four deep",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Optional}}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{}}
	case 3:
		x.Friend = &Entity{Hobby: &Item{Name: &Name{}}}
	case 4:
		x.Friend = &Entity{Hobby: &Item{Name: &Name{First: pstring(vals[nVals])}}}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "four deep mixed",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional, fields.Optional}}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend.Hobby = &Item{}
	case 2:
		x.Friend.Hobby = &Item{Name: &Name{}}
	case 3:
		x.Friend.Hobby = &Item{Name: &Name{First: pstring(vals[nVals])}}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "four deep mixed v2",
			fields: []fields.Field{{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, defs, reps []uint8) (int, int) {
	var nVals int
	def := defs[0]
	switch def {
	case 1:
		x.Friend = &Entity{}
	case 2:
		x.Friend = &Entity{Hobby: &Item{}}
	case 3:
		x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[nVals]}}}
		nVals++
	}

	return nVals, 1
}`,
		},
		{
			name:   "writeLinkBackward",
			fields: []fields.Field{{Type: "Document", TypeName: "int64", FieldNames: []string{"Link", "Backward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}}},
			result: `func writeLinkBackward(x *Document, vals []int64, defs, reps []uint8) (int, int) {
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
				x.Link = append(x.Link, &Link{Backward: []int64{vals[nVals]}})
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
			name: "writeLinkFoward",
			fields: []fields.Field{
				{FieldNames: []string{"Link", "Backward"}},
				{Type: "Document", TypeName: "int64", FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
			},
			result: `func writeLinkForward(x *Document, vals []int64, defs, reps []uint8) (int, int) {
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
			default:
				x.Link.Forward = append(x.Link.Forward, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}`,
		},
		{
			name:   "writeNamesLanguagesCode",
			fields: []fields.Field{{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}}},
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
			fields: []fields.Field{
				{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}},
				{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
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
			default:
				x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])
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
			s := dremel.Write(len(tc.fields)-1, tc.fields)
			gocode, err := format.Source([]byte(s))
			fmt.Println(string(gocode))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
