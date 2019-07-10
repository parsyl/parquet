package dremel_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/internal/dremel"
	"github.com/parsyl/parquet/internal/parse"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	testCases := []struct {
		name   string
		f      parse.Field
		result string
	}{
		{
			name: "required and not nested",
			f:    parse.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"ID"}, RepetitionTypes: []parse.RepetitionType{parse.Required}},
			result: `func readID(x Person) int32 {
	return x.ID
}`,
		},
		{
			name: "optional and not nested",
			f:    parse.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, RepetitionTypes: []parse.RepetitionType{parse.Optional}},
			result: `func readID(x Person) ([]int32, []uint8, []uint8) {
	switch {
	case x.ID == nil:
		return nil, []uint8{0}, nil
	default:
		return []int32{*x.ID}, []uint8{1}, nil
	}
}`,
		},
		{
			name: "required and nested",
			f:    parse.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"Other", "Hobby", "Difficulty"}, FieldTypes: []string{"Other", "Hobby", "int32"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Required, parse.Required}},
			result: `func readOtherHobbyDifficulty(x Person) int32 {
	return x.Other.Hobby.Difficulty
}`,
		},
		{
			name: "optional and nested",
			f:    parse.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional}},
			result: `func readHobbyDifficulty(x Person) ([]int32, []uint8, []uint8) {
	switch {
	case x.Hobby == nil:
		return nil, []uint8{0}, nil
	case x.Hobby.Difficulty == nil:
		return nil, []uint8{1}, nil
	default:
		return []int32{*x.Hobby.Difficulty}, []uint8{2}, nil
	}
}`,
		},
		{
			name: "mix of optional and required and nested",
			f:    parse.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required}},
			result: `func readHobbyName(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Hobby == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{x.Hobby.Name}, []uint8{1}, nil
	}
}`,
		},
		{
			name: "mix of optional and required and nested v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional}},
			result: `func readHobbyName(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Hobby.Name == nil:
		return nil, []uint8{0}, nil
	default:
		return []string{*x.Hobby.Name}, []uint8{1}, nil
	}
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Required, parse.Optional}},
			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby.Name == nil:
		return nil, []uint8{1}, nil
	default:
		return []string{*x.Friend.Hobby.Name}, []uint8{2}, nil
	}
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v2",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional, parse.Optional}},
			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend.Hobby == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby.Name == nil:
		return nil, []uint8{1}, nil
	default:
		return []string{*x.Friend.Hobby.Name}, []uint8{2}, nil
	}
}`,
		},
		{
			name: "mix of optional and require and nested 3 deep v3",
			f:    parse.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Required}},
			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby == nil:
		return nil, []uint8{1}, nil
	default:
		return []string{x.Friend.Hobby.Name}, []uint8{2}, nil
	}
}`,
		},
		{
			name: "nested 3 deep all optional",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional}},
			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby == nil:
		return nil, []uint8{1}, nil
	case x.Friend.Hobby.Name == nil:
		return nil, []uint8{2}, nil
	default:
		return []string{*x.Friend.Hobby.Name}, []uint8{3}, nil
	}
}`,
		},
		{
			name: "four deep",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional, parse.Optional}},
			result: `func readFriendHobbyNameFirst(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby == nil:
		return nil, []uint8{1}, nil
	case x.Friend.Hobby.Name == nil:
		return nil, []uint8{2}, nil
	case x.Friend.Hobby.Name.First == nil:
		return nil, []uint8{3}, nil
	default:
		return []string{*x.Friend.Hobby.Name.First}, []uint8{4}, nil
	}
}`,
		},
		{
			name: "four deep mixed",
			f:    parse.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Required, parse.Optional, parse.Optional, parse.Optional}},
			result: `func readFriendHobbyNameFirst(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend.Hobby == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby.Name == nil:
		return nil, []uint8{1}, nil
	case x.Friend.Hobby.Name.First == nil:
		return nil, []uint8{2}, nil
	default:
		return []string{*x.Friend.Hobby.Name.First}, []uint8{3}, nil
	}
}`,
		},
		{
			name: "four deep mixed v2",
			f:    parse.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []parse.RepetitionType{parse.Optional, parse.Optional, parse.Optional, parse.Required}},
			result: `func readFriendHobbyNameFirst(x Person) ([]string, []uint8, []uint8) {
	switch {
	case x.Friend == nil:
		return nil, []uint8{0}, nil
	case x.Friend.Hobby == nil:
		return nil, []uint8{1}, nil
	case x.Friend.Hobby.Name == nil:
		return nil, []uint8{2}, nil
	default:
		return []string{x.Friend.Hobby.Name.First}, []uint8{3}, nil
	}
}`,
		},
		{
			name: "repeated",
			f:    parse.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Friends"}, FieldTypes: []string{"string"}, RepetitionTypes: []parse.RepetitionType{parse.Repeated}},
			result: `func readFriends(x Person) ([]string, []uint8, []uint8) {
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := dremel.Read(tc.f)
			fmt.Println(s)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, string(gocode))
		})
	}
}
