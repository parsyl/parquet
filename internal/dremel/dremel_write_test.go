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
			f:    parse.Field{TypeName: "int32", FieldNames: []string{"ID"}, Optionals: []bool{false}},
			result: `func writeID(x *Person, v int32, def int64) {
	x.ID = v
}`,
		},
		{
			name: "optional and not nested",
			f:    parse.Field{TypeName: "*int32", FieldNames: []string{"ID"}, Optionals: []bool{true}},
			result: `func writeID(x *Person, v *int32, def int64) {
	x.ID = v
}`,
		},
		{
			name: "optional and nested",
			f:    parse.Field{TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, Optionals: []bool{true, true}},
			result: `func writeHobbyDifficulty(x *Person, v *int32, def int64) {
	switch def {
	case 1:
		if x.Hobby == nil {
			x.Hobby = &Hobby{}
		}
	case 2:
		if x.Hobby == nil {
			x.Hobby = &Hobby{Difficulty: v}
		} else {
			x.Hobby.Difficulty = v
		}
	}
}`,
		},
		{
			name: "mix of optional and require and nested",
			f:    parse.Field{TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, Optionals: []bool{true, false}},
			result: `func writeHobbyDifficulty(x *Person, v *int32, def int64) {
	switch def {
	case 2:
		if x.Hobby == nil {
			x.Hobby = &Hobby{Name: *v}
		} else {
			x.Hobby.Name = *v
		}
	}
}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := dremel.Write(tc.f)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			assert.Equal(t, tc.result, gocode)
		})
	}
}
