package structs_test

import (
	"fmt"
	"go/format"
	"testing"

	"github.com/parsyl/parquet/cmd/parquetgen/structs"
	sch "github.com/parsyl/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func TestStruct(t *testing.T) {
	type testInput struct {
		name     string
		schema   []*sch.SchemaElement
		expected string
	}

	testCases := []testInput{
		{
			name: "single field",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(1)},
				{Name: "id", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
			},
			expected: "type Root struct {\n	Id int32 `parquet:\"id\"`\n}",
		},
		{
			name: "single nested field",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(1)},
				{Name: "hobby", RepetitionType: prt(sch.FieldRepetitionType_REQUIRED), NumChildren: pint32(1)},
				{Name: "name", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
			},
			expected: "type Root struct {\n	Hobby Hobby `parquet:\"hobby\"`\n}\n\ntype Hobby struct {\n	Name *string `parquet:\"name\"`\n}",
		},
		{
			name: "nested 3 deep",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(2)},
				{Name: "hobby", RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL), NumChildren: pint32(2)},
				{Name: "name", RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL), NumChildren: pint32(2)},
				{Name: "first", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
				{Name: "last", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
				{Name: "difficulty", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
				{Name: "id", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
			},
			expected: "type Root struct {\n	Hobby *Hobby `parquet:\"hobby\"`\n	Id    int32  `parquet:\"id\"`\n}\n\ntype Hobby struct {\n	Name       *Name  `parquet:\"name\"`\n	Difficulty *int32 `parquet:\"difficulty\"`\n}\n\ntype Name struct {\n	First *string `parquet:\"first\"`\n	Last  string  `parquet:\"last\"`\n}",
		},
		{
			name: "nested 3 deep v2",
			schema: []*sch.SchemaElement{
				{Name: "root", NumChildren: pint32(2)},
				{Name: "hobby", RepetitionType: prt(sch.FieldRepetitionType_REQUIRED), NumChildren: pint32(2)},
				{Name: "name", RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL), NumChildren: pint32(2)},
				{Name: "first", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
				{Name: "last", Type: pt(sch.Type_BYTE_ARRAY), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
				{Name: "difficulty", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_REQUIRED)},
				{Name: "id", Type: pt(sch.Type_INT32), RepetitionType: prt(sch.FieldRepetitionType_OPTIONAL)},
			},
			expected: "type Root struct {\n	Hobby Hobby  `parquet:\"hobby\"`\n	Id    *int32 `parquet:\"id\"`\n}\n\ntype Hobby struct {\n	Name       *Name `parquet:\"name\"`\n	Difficulty int32 `parquet:\"difficulty\"`\n}\n\ntype Name struct {\n	First *string `parquet:\"first\"`\n	Last  string  `parquet:\"last\"`\n}",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
			s := structs.Struct("Root", tc.schema)
			gocode, err := format.Source([]byte(s))
			assert.NoError(t, err)
			if !assert.Equal(t, tc.expected, string(gocode)) {
				t.Fatal(string(gocode))
			}
		})
	}
}

func pint32(i int32) *int32 {
	return &i
}

func prt(rt sch.FieldRepetitionType) *sch.FieldRepetitionType {
	return &rt
}

func pt(t sch.Type) *sch.Type {
	return &t
}
