package dremel

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/fields"
)

var (
	writeRepeatedTpl *template.Template
	ifTpl            *template.Template
)

type defCase struct {
	Def   int
	Seen  []fields.RepetitionType
	Field fields.Field
}

type writeRepeatedInput struct {
	Field fields.Field
	Defs  []int
	Func  string
}

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
		},
		"newDefCase": func(def int, f fields.Field) defCase {
			return defCase{Def: def, Field: f}
		},
		"init": initRepeated,
		"getRep": func(def int, f fields.Field) int {
			var rep int
			//defindex indead of def?
			for _, rt := range f.RepetitionTypes()[:f.DefIndex(def)] {
				if rt == fields.Repeated {
					rep++
				}
			}
			return rep
		},
		"notNil": func(x *ifElse) bool { return x != nil },
	}

	var err error
	ifTpl, err = template.New("tmp").Funcs(funcs).Parse(`{{template "ifelse" .}}`)
	if err != nil {
		log.Fatalf("unable to create templates: %s", err)
	}
	ifTpl, err = ifTpl.Parse(ifelseStmt)
	if err != nil {
		log.Fatalf("unable to create templates: %s", err)
	}

	writeRepeatedTpl, err = template.New("output").Funcs(funcs).Parse(`func {{.Func}}(x *{{.Field.StructType}}, vals []{{removeStar .Field.TypeName}}, defs, reps []uint8) (int, int) {
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
				{{ template "defCase" newDefCase $def $.Field}}{{if eq $def $.Field.MaxDef}}
				nVals++{{end}}{{end}}			
		}{{end}}`

	defCaseTpl := `{{define "defCase"}}switch rep {
{{range $case := .Field.RepCases $.Def}}{{$case.Case}}
	{{init $.Def $case.Rep $.Field}}
{{end}} }{{end}}`

	for _, t := range []string{defCaseTpl, defSwitchTpl} {
		writeRepeatedTpl, err = writeRepeatedTpl.Parse(t)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func writeRepeated(f fields.Field) string {
	wi := writeRepeatedInput{
		Field: f,
		Func:  fmt.Sprintf("write%s", strings.Join(f.FieldNames(), "")),
		Defs:  writeCases(f),
	}

	var buf bytes.Buffer
	if err := writeRepeatedTpl.Execute(&buf, wi); err != nil {
		fmt.Println(err)
		return ""
	}
	return string(buf.Bytes())
}

func initRepeated(def, rep int, f fields.Field) string {
	md := int(f.MaxDef())
	rt := f.RepetitionTypes().Def(def)

	if def < md && rep == 0 && rt == fields.Repeated {
		rep = def
	}

	return f.Init(def, rep)
}

func writeCases(f fields.Field) []int {
	var out []int
	md := f.MaxDef()
	chain := fields.Reverse(f.Chain())
	start := 1
	for _, f := range chain {
		if f.RepetitionType != fields.Required && f.Defined && start < md {
			start++
		}
	}

	for def := start; def <= md; def++ {
		out = append(out, def)
	}
	return out
}
