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

type defCase struct {
	Def   int
	Seen  int
	Field fields.Field
}

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
		},
		"plusOne":    func(i int) int { return i + 1 },
		"newDefCase": func(def, seen int, f fields.Field) defCase { return defCase{Def: def, Seen: seen, Field: f} },
		"init": func(def, rep, seen int, f fields.Field) string {
			if def < f.MaxDef() {
				//calculate what rep should be
				for _, rt := range f.RepetitionTypes[:def] {
					if rt == fields.Repeated {
						rep++
					}
				}
			}
			return structs.Init(def, rep, seen, f)
		},
		"repeat": func(def int, f fields.Field) bool { return f.Repeated() && def == f.MaxDef() },
	}

	var err error
	writeRepeatedTpl, err = template.New("output").Funcs(funcs).Parse(`func {{.Func}}(x *{{.Field.Type}}, vals []{{removeStar .Field.TypeName}}, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, {{.Field.MaxRep}})

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		{{template "defSwitch" .}}
	}

	return nVals, nLevels
}`)
	if err != nil {
		log.Fatalf("unable to create templates: %s", err)
	}

	defSwitchTpl := `{{define "defSwitch"}}switch def { {{range $i, $def := .Defs}}
			case {{$def}}:
				{{ template "defCase" newDefCase $def $.Seen $.Field}}{{if eq $def $.Field.MaxDef}}
				nVals++{{end}}{{end}}			
		}{{end}}`

	defCaseTpl := `{{define "defCase"}}{{if repeat .Def .Field}}{{ template "repSwitch" .}}{{else}}{{init .Def 0 .Seen .Field}}{{end}}{{end}}`

	repSwitchTpl := `{{define "repSwitch"}}switch rep {
{{range $case := .Field.RepCases $.Seen}}{{$case.Case}}
{{init $.Def $case.Rep $.Seen $.Field}}
{{end}} } {{end}}`

	for _, t := range []string{defCaseTpl, defSwitchTpl, repSwitchTpl} {
		writeRepeatedTpl, err = writeRepeatedTpl.Parse(t)
		if err != nil {
			log.Fatal(err)
		}
	}
}

var (
	writeRepeatedTpl *template.Template
)

type writeRepeatedInput struct {
	Field fields.Field
	Defs  []int
	Seen  int
	Func  string
}

func writeRequired(f fields.Field) string {
	return fmt.Sprintf(`func %s(x *%s, vals []%s) {
	x.%s = vals[0]
}`, fmt.Sprintf("write%s", strings.Join(f.FieldNames, "")), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
}

func writeRepeated(i int, fields []fields.Field) string {
	f := fields[i]
	s := seen(i, fields)
	defs := writeCases(f, s)
	wi := writeRepeatedInput{
		Field: f,
		Func:  fmt.Sprintf("write%s", strings.Join(f.FieldNames, "")),
		Defs:  defs,
		Seen:  s,
	}

	var buf bytes.Buffer
	err := writeRepeatedTpl.Execute(&buf, wi)
	if err != nil {
		log.Fatal(err) //TODO: return error
	}
	return string(buf.Bytes())
}

func writeCases(f fields.Field, seen int) []int {
	var dfs []int
	for def := 1 + seen; def <= f.MaxDef(); def++ {
		dfs = append(dfs, def)
	}
	return dfs
}

func nilField(i int, f fields.Field) string {
	var flds []string
	var count int
	for j, o := range f.RepetitionTypes {
		flds = append(flds, f.FieldNames[j])
		if o == fields.Optional {
			count++
		}
		if count > i {
			break
		}
	}
	return strings.Join(flds, ".")
}

func defIndex(i int, f fields.Field) int {
	var count int
	for j, o := range f.RepetitionTypes {
		if o == fields.Optional || o == fields.Repeated {
			count++
		}
		if count > i {
			return j
		}
	}
	return -1
}

// count the number of fields in the path that can be optional
func defs(f fields.Field) int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == fields.Optional || o == fields.Repeated {
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
func seen(i int, fields []fields.Field) int {
	m := map[string]int{}
	for _, ft := range fields[i].FieldNames {
		m[ft] = 1
	}

	for _, f := range fields[:i] {
		if len(f.FieldNames) == 1 {
			continue
		}

		for _, fn := range f.FieldNames {
			_, ok := m[fn]
			if ok {
				m[fn]++
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
