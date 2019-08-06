package dremel_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/dremel"
	"github.com/parsyl/parquet/internal/fields"
	"github.com/stretchr/testify/assert"
)

func TestWriteOptional(t *testing.T) {
	testCases := []struct {
		name   string
		f      fields.Field
		result string
	}{
		{
			name: "required and not nested",
			f:    fields.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"ID"}, RepetitionTypes: []fields.RepetitionType{fields.Required}},
			result: `func writeID(x *Person, vals []int32) {
	x.ID = vals[0]
}`,
		},
		{
			name: "optional and not nested",
			f:    fields.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"Other", "Hobby", "Difficulty"}, FieldTypes: []string{"Other", "Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Required, fields.Required}},
			result: `func writeOtherHobbyDifficulty(x *Person, vals []int32) {
	x.Other.Hobby.Difficulty = vals[0]
}`,
		},
		{
			name: "optional and nested",
			f:    fields.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Required}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional, fields.Optional}},
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
			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
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
			x.Friend = &Entity{Hobby: &Item{Name: &Name{First: vals[nVals]}}}
		} else if x.Friend.Hobby == nil {
			x.Friend.Hobby = &Item{Name: &Name{First: vals[nVals]}}
		} else {
			x.Friend.Hobby.Name.First = vals[nVals]
		}
		return 1, 1
	}

	return 0, 1
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := dremel.Write(0, []fields.Field{tc.f})
			fmt.Println(s)
			gocode, err := format.Source([]byte(s))
			fmt.Println(string(gocode))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
