package dremel

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/parse"
	"github.com/parsyl/parquet/internal/structs"
)

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(s, "*", "", 1)
		},
		"plusOne": func(i int) int { return i + 1 },
	}

	var err error
	writeTpl, err = template.New("output").Funcs(funcs).Parse(`func {{.Func}}(x *{{.Type}}, vals []{{.TypeName}}, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	defs, reps, nLevels = getDocLevels(defs, reps)

	{{if gt .Seen 0}}ind := indices(make([]int, {{.Seen}})){{end}}
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		{{if gt .Seen 0}}ind.rep(rep){{end}}
		switch def { {{range $i, $case := .Defs}}
			case $case:
				{{index .Cases $i}}{{end}}
		}
	}

	return nVals, nLevels
}`)
	if err != nil {
		log.Fatal(err)
	}
}

var (
	writeTpl *template.Template
)

type writeInput struct {
	parse.Field
	Cases []string
	Defs  []int
	Seen  int
	Func  string
}

func writeRequired(f parse.Field) string {
	return fmt.Sprintf(`func %s(x *%s, vals []%s) {
	x.%s = vals[0]
}`, fmt.Sprintf("write%s", strings.Join(f.FieldNames, "")), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
}

func writeOptional(i int, fields []parse.Field) string {
	f := fields[i]
	s := seen(i, fields)
	cs, defs := writeCases(f, s)
	wi := writeInput{
		Field: f,
		Func:  fmt.Sprintf("write%s", strings.Join(f.FieldNames, "")),
		Cases: cs,
		Defs:  defs,
		Seen:  s,
	}

	var buf bytes.Buffer
	err := writeTpl.Execute(&buf, wi)
	if err != nil {
		log.Fatal(err) //TODO: return error
	}
	return string(buf.Bytes())
}

func writeCases(f parse.Field, seen int) ([]string, []int) {
	var cases []string
	var dfs []int
	for def := seen + 1; def <= defs(f); def++ {
		dfs = append(dfs, def)
		var val, inc string
		if def == defs(f) {
			val = `v := vals[0]
		`
			inc = `
	nVals++
	`
		}

		cases = append(cases, fmt.Sprintf(`%s%s%s`, val, ifelse(0, def, f), inc))
	}
	return cases, dfs
}

// return an if else block for the definition level
func ifelse(i, def int, f parse.Field) string {
	fmt.Printf("if else i: %d, def: %d, optionals: %d, field: %+v\n", i, def, optionals(f), f)
	if i == recursions(def, f) || def > optionals(f) {
		return ""
	}

	var stmt, brace, val, field, cmp string
	rt := f.RepetitionTypes[len(f.RepetitionTypes)-1]
	if i == 0 && defs(f) == 1 && rt == parse.Optional {
		return fmt.Sprintf(`x.%s = &v`, strings.Join(f.FieldNames, "."))
	}

	if i == 0 {
		stmt = "if"
		brace = "}"
		field = fmt.Sprintf("x.%s", nilField(i, f))
		ch := f.Child(defIndex(i, f))
		cmp = fmt.Sprintf(" x.%s == nil", nilField(i, f))
		val = structs.Init(def, 0, ch)
	} else if i > 0 && i < defs(f)-1 {
		stmt = " else if"
		brace = "}"
		cmp = fmt.Sprintf(" x.%s == nil", nilField(i, f))
		ch := f.Child(defIndex(i, f))
		val = structs.Init(def-i, 0, ch)
		field = fmt.Sprintf("x.%s", nilField(i, f))
	} else {
		stmt = " else"
		val = "v"
		if f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Optional {
			val = "&v"
		}
		brace = "}"
		field = fmt.Sprintf("x.%s", strings.Join(f.FieldNames, "."))
	}

	return fmt.Sprintf(`%s%s {
	%s = %s
	%s%s`, stmt, cmp, field, val, brace, ifelse(i+1, def, f))
}

// recursions calculates the number of times ifelse should execute
func recursions(def int, f parse.Field) int {
	n := def
	if defs(f) == 1 {
		n++
	}
	return n
}

func nilField(i int, f parse.Field) string {
	var fields []string
	var count int
	for j, o := range f.RepetitionTypes {
		fields = append(fields, f.FieldNames[j])
		if o == parse.Optional {
			count++
		}
		if count > i {
			break
		}
	}
	return strings.Join(fields, ".")
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
	return -1
}

// count the number of fields in the path that can be optional
func defs(f parse.Field) int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == parse.Optional || o == parse.Repeated {
			out++
		}
	}
	return out
}

func optionals(f parse.Field) int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == parse.Optional {
			out++
		}
	}
	return out
}

func pointer(i, n int, p string, levels []bool) string {
	if levels[n] && n < i {
		return p
	}
	return ""
}

// seen counts how many sub-fields have been previously processed
// so that some of the cases and if statements can be skipped when
// re-assembling records
func seen(i int, fields []parse.Field) int {
	m := map[string]int{}
	for _, ft := range fields[i].FieldTypes {
		m[ft] = 1
	}

	for _, f := range fields[:i] {
		for _, ft := range f.FieldTypes {
			_, ok := m[ft]
			if ok {
				m[ft]++
			}
		}
	}

	var out int
	for _, i := range m {
		if i > 1 {
			out++
		}
	}

	return out
}
