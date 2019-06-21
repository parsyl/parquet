package structs

import (
	"fmt"
	"strings"

	sch "github.com/parsyl/parquet/generated"
	"github.com/parsyl/parquet/internal/parse"
)

// Init initializes the nested structs according to the
// current definition level.
func Init(def int, f parse.Field) string {
	return doInit(def, 0, f)
}

func doInit(def, i int, f parse.Field) string {
	var j int
	for _, o := range f.RepetitionTypes[:i+1] {
		if o == parse.Optional {
			j++
		}
	}

	if def == nDefs(f) && i == len(f.RepetitionTypes)-1 {
		var ptr string
		if f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Optional {
			ptr = "&"
		}
		return fmt.Sprintf("%s: %sv", f.FieldNames[i], ptr)
	}

	if i == def && def < nDefs(f) {
		return ""
	}

	var field string
	if i > 0 && i < len(f.RepetitionTypes) {
		field = fmt.Sprintf("%s: ", f.FieldNames[i])
	}

	var typ string
	var ptr string
	if i < nDefs(f) {
		typ = f.FieldTypes[i]
		if f.RepetitionTypes[i] == parse.Optional {
			ptr = "&"
		}
	}

	return fmt.Sprintf("%s%s%s{%s}", field, ptr, typ, doInit(def, i+1, f))
}

func nDefs(f parse.Field) int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == parse.Optional {
			out++
		}
	}
	return out
}

// Struct generates a struct definition based on the
// parquet schema.
func Struct(structName string, schema []*sch.SchemaElement) string {
	if len(schema) == 0 {
		return ""
	}

	schema[0].Name = structName
	_, out := getStruct(schema[0], schema[1:])
	if strings.Contains(out, "%s") {
		out = fmt.Sprintf(out, "")
	}

	return out
}

func getStruct(parent *sch.SchemaElement, children []*sch.SchemaElement) (int, string) {
	str := fmt.Sprintf(`type %s struct {
	%%s
}`, strings.Title(parent.Name))
	var i, j int
	var fields string
	for i < int(*parent.NumChildren) {
		ch := children[i+j]
		fields = fmt.Sprintf("%s\n%s", fields, field(ch))
		if ch.NumChildren != nil && int(*ch.NumChildren) > 0 {
			n, s := getStruct(ch, children[i+j+1:])
			j += n
			str += fmt.Sprintf("\n\n%s", s)
		}
		i++
	}

	return i + j, fmt.Sprintf(str, fields)
}

func field(elem *sch.SchemaElement) string {
	n := strings.Title(elem.Name)
	t := n
	if elem.Type != nil {
		t = getType(elem.Type.String())
	}
	var ptr string
	if elem.RepetitionType != nil && *elem.RepetitionType == sch.FieldRepetitionType_OPTIONAL {
		ptr = "*"
	}
	return fmt.Sprintf("%s %s%s", n, ptr, t)
}

func getType(t string) string {
	return parquetTypes[t]
}

var parquetTypes = map[string]string{
	"BOOLEAN":    "bool",
	"INT32":      "int32",
	"INT64":      "int64",
	"FLOAT":      "float32",
	"DOUBLE":     "float64",
	"BYTE_ARRAY": "string",
}
