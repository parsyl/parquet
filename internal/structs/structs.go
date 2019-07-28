package structs

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/parse"
	sch "github.com/parsyl/parquet/schema"
)

// Init generates the statement to append a value based
// on the definition and repetition level
func Init(def, rep int, f parse.Field) string {
	fmt.Println("init", def, rep, f)
	if !f.Repeated() {
		return fmt.Sprintf("x.%s = %s", f.FieldNames[0], doInit(def, rep, 0, f, "v"))
	}

	// zero means start of a record, so that means the first field is really the one being repeated
	if rep == 0 {
		rep++
	}

	var names []string
	var repeats []bool
	var count int

	for i, n := range f.FieldNames {
		var r bool
		if f.RepetitionTypes[i] == parse.Repeated {
			count++
			r = true
		}
		names = append(names, n)
		repeats = append(repeats, r)
		if count == rep {
			break
		}
	}

	names = addIndex(names, repeats)

	s := strings.Join(names, ".")

	var val string
	if def == f.MaxDef() && rep == nReps(f) && f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Repeated {
		val = "vals[nVals]"
	} else {
		i := len(names) - 1
		def -= nDefs(f.RepetitionTypes[:i])
		f = f.Child(i)
		val = doInit(def, rep, 0, f, "vals[nVals]")
	}
	return fmt.Sprintf("x.%s = append(x.%s, %s)", s, s, val)
}

func addIndex(names []string, repeats []bool) []string {
	c := nRepeats(repeats)
	var seen int
	for i := range names {
		r := repeats[i]
		if r {
			seen++
		}

		if r && seen < c {
			end := i + 1
			if end >= len(names) {
				end = len(names) - 1
			}
			f := strings.Join(names[:end], ".")
			names = append([]string{fmt.Sprintf("%s[len(x.%s)-1]", f, f)}, names[end:]...)
		}
	}

	return names
}

func nRepeats(repeats []bool) int {
	var out int
	for _, r := range repeats {
		if r {
			out++
		}
	}
	return out
}

func initOptional(def int, f parse.Field) string {
	return doInit(def, 0, 0, f, "v")
}

func doInit(def, rep, i int, f parse.Field, val string) string {
	maxDef := f.MaxDef()
	if def == maxDef && i == len(f.RepetitionTypes)-1 {
		var ptr string
		if f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Optional {
			ptr = "&"
		}
		if f.RepetitionTypes[i] == parse.Repeated {
			val = fmt.Sprintf("[]%s{%s}", f.FieldTypes[len(f.FieldTypes)-1], val)
		}
		return fmt.Sprintf("%s: %s%s", f.FieldNames[i], ptr, val)
	}

	if i == def && def < maxDef {
		return ""
	}

	var field string
	if i > 0 && i < len(f.RepetitionTypes) {
		field = fmt.Sprintf("%s: ", f.FieldNames[i])
	}

	var typ string
	var ptr string
	leftBrace := "{"
	rightBrace := "}"
	if i < nDefs(f.RepetitionTypes) || def == 0 {
		typ = f.FieldTypes[i]
		if f.RepetitionTypes[i] == parse.Optional {
			ptr = "&"
		} else if f.RepetitionTypes[i] == parse.Repeated && !isBeingRepeated(f, rep, i) {
			ptr = "[]"
			leftBrace = "{{"
			rightBrace = "}}"
		}
	}

	return fmt.Sprintf("%s%s%s%s%s%s", field, ptr, typ, leftBrace, doInit(def, rep, i+1, f, val), rightBrace)
}

func isBeingRepeated(f parse.Field, rep, i int) bool {
	var reps int
	for _, rt := range f.RepetitionTypes[:i] {
		if rt == parse.Repeated {
			reps++
		}
	}
	return reps < rep
}

func nDefs(rt []parse.RepetitionType) int {
	var out int
	for _, o := range rt {
		if o == parse.Optional || o == parse.Repeated {
			out++
		}
	}
	return out
}

func nReps(f parse.Field) int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == parse.Repeated {
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
