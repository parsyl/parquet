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
		f      parse.Field
		result string
	}{
		{
			name: "required and not nested",
			f:    parse.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"ID"}, Optionals: []bool{false}},
			result: `func writeID(x *Person, vals []int32, def int64) bool {
	x.ID = vals[0]
	return true
}`,
		},
		{
			name: "optional and not nested",
			f:    parse.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, Optionals: []bool{true}},
			result: `func writeID(x *Person, vals []int32, def int64) bool {
	if def == 0 {
		return false
	}

	v := vals[0]
	x.ID = &v
	return true
}`,
		},
		{
			name: "required and nested",
			f:    parse.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"Other", "Hobby", "Difficulty"}, FieldTypes: []string{"Other", "Hobby", "int32"}, Optionals: []bool{false, false, false}},
			result: `func writeOtherHobbyDifficulty(x *Person, vals []int32, def int64) bool {
	x.Other.Hobby.Difficulty = vals[0]
	return true
}`,
		},
		{
			name: "optional and nested",
			f:    parse.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, Optionals: []bool{true, true}},
			result: `func writeHobbyDifficulty(x *Person, vals []int32, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "mix of optional and required and nested",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, Optionals: []bool{true, false}},
			result: `func writeHobbyName(x *Person, vals []string, def int64) bool {
	if def == 0 {
		return false
	}

	v := vals[0]
	if x.Hobby == nil {
		x.Hobby = &Hobby{Name: v}
	} else {
		x.Hobby.Name = v
	}
	return true
}`,
		},
		{
			name: "mix of optional and require and nested v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, Optionals: []bool{false, true}},
			result: `func writeHobbyName(x *Person, vals []string, def int64) bool {
	if def == 0 {
		return false
	}

	v := vals[0]
	x.Hobby.Name = &v
	return true
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, Optionals: []bool{true, false, true}},
			result: `func writeFriendHobbyName(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, Optionals: []bool{false, true, true}},
			result: `func writeFriendHobbyName(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v3",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, Optionals: []bool{true, true, false}},
			result: `func writeFriendHobbyName(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "nested 3 deep all optional",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, Optionals: []bool{true, true, true}},
			result: `func writeFriendHobbyName(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "four deep",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, Optionals: []bool{true, true, true, true}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "four deep mixed",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, Optionals: []bool{false, true, true, true}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
		{
			name: "four deep mixed v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, Optionals: []bool{true, true, true, false}},
			result: `func writeFriendHobbyNameFirst(x *Person, vals []string, def int64) bool {
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
		return true
	}
	return false
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := dremel.Write(tc.f)
			fmt.Println(s)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
