package dremel

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/fields"
	"github.com/parsyl/parquet/internal/structs"
)

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(s, "*", "", 1)
		},
	}

	var err error
	writeTpl, err = template.New("output").Funcs(funcs).Parse(`func write{{.FuncName}}(x *{{.Field.Type}}, vals []{{removeStar .Field.TypeName}}, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def { {{range .Cases}}
	{{.}}{{end}} }
	return 0, 1
}`)
	if err != nil {
		log.Fatal(err)
	}

	writeTpl, err = writeTpl.Parse(`{{define "initStructs"}}{{range .}}{{.}}{{end}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}
}

var (
	writeTpl *template.Template
)

type writeInput struct {
	fields.Field
	Cases    []string
	FuncName string
}

func writeOptional(f fields.Field) string {
	i := writeInput{
		Field:    f,
		FuncName: strings.Join(f.FieldNames, ""),
		Cases:    writeOptionalCases(f),
	}

	var buf bytes.Buffer
	err := writeTpl.Execute(&buf, i)
	if err != nil {
		log.Fatal(err) //TODO: return error
	}
	return string(buf.Bytes())
}

func writeOptionalCases(f fields.Field) []string {
	var out []string
	for def := 1; def <= defs(f); def++ {
		var v, ret string
		if def == defs(f) {
			v = `v := vals[0]
		`
			ret = `
	return 1, 1
	`
		}

		cs := fmt.Sprintf(`case %d:
	`, def)

		out = append(out, fmt.Sprintf(`%s%s%s%s`, cs, v, ifelse(0, def, f), ret))
	}
	return out
}

// return an if else block for the definition level
func ifelse(i, def int, f fields.Field) string {
	if i == recursions(def, f) {
		return ""
	}

	var stmt, brace, val, cmp string
	if i == 0 && defs(f) == 1 && (f.RepetitionTypes[len(f.RepetitionTypes)-1] == fields.Optional) {
		return fmt.Sprintf(`x.%s = &v`, strings.Join(f.FieldNames, "."))
	} else if i == 0 {
		stmt = "if"
		brace = "}"
		//field = fmt.Sprintf("x.%s", nilField(i, f))
		cmp = fmt.Sprintf(" x.%s == nil", nilField(i, f))
		//ch := f.Child(defIndex(i, f))
		val = structs.Init(def, 0, 0, f.DefChild(i))
	} else if i > 0 && i < defs(f)-1 {
		stmt = " else if"
		brace = "}"
		cmp = fmt.Sprintf(" x.%s == nil", nilField(i, f))
		//ch := f.Child(defIndex(i, f))
		val = structs.Init(def, 0, 0, f.DefChild(i))
		//field = fmt.Sprintf("x.%s", nilField(i, f))
	} else {
		stmt = " else"
		val = "v"
		if f.RepetitionTypes[len(f.RepetitionTypes)-1] == fields.Optional {
			val = "&v"
		}
		brace = "}"
		//field = fmt.Sprintf("x.%s", strings.Join(f.FieldNames, "."))
	}

	return fmt.Sprintf(`%s%s {
	%s
	%s%s`, stmt, cmp, val, brace, ifelse(i+1, def, f))
}

// recursions calculates the number of times ifelse should execute
func recursions(def int, f fields.Field) int {
	n := def
	if defs(f) == 1 {
		n++
	}
	return n
}
