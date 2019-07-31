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
	if rep == 0 || !f.Repeated() {
		if f.Required() {
			return fmt.Sprintf("x.%s = %s", strings.Join(f.FieldNames, "."), "vals[nVals]")
		}

		i := f.DefIndex(1)
		ch := f.Child(i)
		append := ch.RepetitionTypes[0] == parse.Repeated && def < f.MaxDef()
		n := strings.Join(f.FieldNames[:i+1], ".")
		if append {
			return fmt.Sprintf("x.%s = append(x.%s, %s)", n, n, doInit(def, rep, 0, ch, true))
		}
		return fmt.Sprintf("x.%s = %s", n, doInit(def, rep, 0, ch, false))
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
	tpl := "x.%s = append(x.%s, %s)"
	s2 := s

	i, rt := f.NthDef(def)

	if def == f.MaxDef() && rep == nReps(f) && f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Repeated {
		val = "vals[nVals]"
	} else if rt != parse.Repeated {
		tpl = "x.%s%s = %s"
		s2 = ""
		//def -= nDefs(f.RepetitionTypes[:i])
		names = names[:i]
		s = strings.Join(names, ".")
		f = f.Child(i - 1)
		val = doInit(def, rep, 0, f, false)
	} else {
		i := len(names) - 1
		def -= nDefs(f.RepetitionTypes[:i])
		f = f.Child(i)
		val = doInit(def, rep, 0, f, true)
	}
	return fmt.Sprintf(tpl, s, s2, val)
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
			names = append([]string{fmt.Sprintf("%s[ind[%d]]", f, seen-1)}, names[end:]...)
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
	return doInit(def, 0, 0, f, false)
}

func defIndex(i int, f parse.Field) int {
	var count int
	for j, o := range f.RepetitionTypes {
		if o == parse.Optional || o == parse.Repeated {
			count++
		}
		if count > i {
			return j
		}
	}
	return i
}

func primitive(typ string) bool {
	return primitiveTypes[typ]
}

func doInit(def, rep, i int, f parse.Field, append bool) string {
	if len(f.FieldNames) == 0 {
		return ""
	}

	val := "vals[nVals]"
	if i >= def && f.MaxDef() > 0 {
		return ""
	}

	if len(f.FieldNames) == 1 {
		if f.RepetitionTypes[0] == parse.Optional && !primitive(f.FieldTypes[0]) {
			val = fmt.Sprintf("&%s{}", f.FieldTypes[0])
		} else if f.RepetitionTypes[0] == parse.Required && !primitive(f.FieldTypes[0]) {
			val = fmt.Sprintf("%s{}", f.FieldTypes[0])
		} else if f.RepetitionTypes[0] == parse.Optional && primitive(f.FieldTypes[0]) {
			val = fmt.Sprintf("p%s(%s)", f.FieldTypes[0], val)
		} else if f.RepetitionTypes[0] == parse.Repeated && !primitive(f.FieldTypes[0]) {
			val = fmt.Sprintf("[]%s{{%s}}", f.FieldTypes[0], val)
		} else if f.RepetitionTypes[0] == parse.Repeated && primitive(f.FieldTypes[0]) {
			val = fmt.Sprintf("[]%s{%s}", f.FieldTypes[0], val)
		}

		var fieldName string
		if i > 0 {
			fieldName = fmt.Sprintf("%s: ", f.FieldNames[0])
		}
		s := fmt.Sprintf("%s%s", fieldName, val)
		return s
	}

	var field string
	if i > 0 {
		field = fmt.Sprintf("%s: ", f.FieldNames[0])
	}

	var typ string
	var ptr string
	leftBrace := "{"
	rightBrace := "}"
	typ = f.FieldTypes[0]
	if f.RepetitionTypes[0] == parse.Optional {
		ptr = "&"
	} else if f.RepetitionTypes[0] == parse.Repeated && !append {
		ptr = "[]"
		leftBrace = "{{"
		rightBrace = "}}"
	}

	if f.RepetitionTypes[0] != parse.Required {
		i++
	}

	return fmt.Sprintf("%s%s%s%s%s%s", field, ptr, typ, leftBrace, doInit(def, rep, i, f.Child(1), false), rightBrace)
}

func isBeingAppended(f parse.Field, rep int, i int) bool {
	var reps int
	for _, rt := range f.RepetitionTypes {
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

var primitiveTypes = map[string]bool{
	"bool":    true,
	"int32":   true,
	"int64":   true,
	"float32": true,
	"float64": true,
	"string":  true,
}
