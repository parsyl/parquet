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

type defCase struct {
	Def   int
	Seen  int
	Field parse.Field
}

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
		},
		"plusOne":    func(i int) int { return i + 1 },
		"newDefCase": func(def, seen int, f parse.Field) defCase { return defCase{Def: def, Seen: seen, Field: f} },
		"init": func(def, rep int, f parse.Field) string {
			return structs.Init(def, rep, f)
		},
		"repeat": func(def int, f parse.Field) bool { return f.Repeated() && def == f.MaxDef() },
	}

	var err error
	writeTpl, err = template.New("output").Funcs(funcs).Parse(`func {{.Func}}(x *{{.Field.Type}}, vals []{{removeStar .Field.TypeName}}, defs, reps []uint8) (int, int) {
	{{if .Field.Repeated}}{{template "repeated" .}}{{else}}{{template "notRepeated" .}}{{end}}	
}`)
	if err != nil {
		log.Fatal(err)
	}

	notRepeatedTpl := `{{define "notRepeated"}}var nVals int
	def := defs[0]
	{{template "defSwitch" .}}

	return nVals, 1{{end}}`

	repeatedTpl := `{{define "repeated"}}var nVals, nLevels int

	{{if gt .Seen 1}}ind := indices(make([]int, {{.Seen}})){{end}}
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		{{if gt .Seen 1}}ind.rep(rep){{end}}
		{{template "defSwitch" .}}
	}

	return nVals, nLevels{{end}}`

	defSwitchTpl := `{{define "defSwitch"}}switch def { {{range $i, $def := .Defs}}
			case {{$def}}:
				{{ template "defCase" newDefCase $def $.Seen $.Field}}{{if eq $def $.Field.MaxDef}}
				nVals++{{end}}{{end}}			
		}{{end}}`

	defCaseTpl := `{{define "defCase"}}{{if repeat .Def .Field}}{{ template "repSwitch" .}}{{else}}{{init .Def 0 .Field}}{{end}}{{end}}`

	repSwitchTpl := `{{define "repSwitch"}}switch rep {
{{range $case := .Field.RepCases $.Seen}}{{$case.Case}}
{{init $.Def $case.Rep $.Field}}
{{end}} } {{end}}`

	for _, t := range []string{notRepeatedTpl, repeatedTpl, defSwitchTpl, defCaseTpl, repSwitchTpl} {
		writeTpl, err = writeTpl.Parse(t)
		if err != nil {
			log.Fatal(err)
		}
	}
}

var (
	writeTpl *template.Template
)

type writeInput struct {
	Field parse.Field
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
	defs := writeCases(f, s)
	wi := writeInput{
		Field: f,
		Func:  fmt.Sprintf("write%s", strings.Join(f.FieldNames, "")),
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

func writeCases(f parse.Field, seen int) []int {
	var dfs []int
	for def := 1 + seen; def <= f.MaxDef(); def++ {
		dfs = append(dfs, def)
	}
	return dfs
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
	for _, ft := range fields[i].FieldNames {
		m[ft] = 1
	}

	for _, f := range fields[:i] {
		for _, ft := range f.FieldNames {
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
