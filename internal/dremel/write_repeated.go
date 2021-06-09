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

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
		},
		"newDefCase": func(def int, seen []fields.RepetitionType, f fields.Field) defCase {
			return defCase{Def: def, Seen: seen, Field: f}
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
				{{ template "defCase" newDefCase $def $.Seen $.Field}}{{if eq $def $.Field.MaxDef}}
				nVals++{{end}}{{end}}			
		}{{end}}`

	defCaseTpl := `{{define "defCase"}}{{if eq .Def .Field.MaxDef}}{{template "repSwitch" .}}{{else}}{{$rep:=getRep .Def .Field}}{{init .Def $rep .Seen .Field}}{{end}}{{end}}`

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

type writeRepeatedInput struct {
	Field fields.Field
	Defs  []int
	Func  string
}

func writeRequired(f fields.Field) string {
	return fmt.Sprintf(`func %s(x *%s, vals []%s) {
	x.%s = vals[0]
}`, fmt.Sprintf("write%s", strings.Join(f.FieldNames(), "")), f.StructType(), f.TypeName, strings.Join(f.FieldNames(), "."))
}

func writeRepeated(f fields.Field) string {
	wi := writeRepeatedInput{
		Field: f,
		Func:  fmt.Sprintf("write%s", strings.Join(f.FieldNames(), "")),
		Defs:  writeCases(f),
	}

	var buf bytes.Buffer
	writeRepeatedTpl.Execute(&buf, wi)
	return string(buf.Bytes())
}

func initRepeated(def, rep int, seen fields.RepetitionTypes, f fields.Field) string {
	md := int(f.MaxDef())
	rt := f.RepetitionTypes().Def(def)

	if def < md && rep == 0 && rt == fields.Repeated {
		rep = def
	}

	if useIfElse(def, rep, f) {
		ie := ifelses(def, rep, f)
		var buf bytes.Buffer
		if err := ifTpl.Execute(&buf, ie); err != nil {
			log.Fatalf("unable to execute ifTpl: %s", err)
		}
		return string(buf.Bytes())
	}

	return f.Init(def, rep)
}

func useIfElse(def, rep int, f fields.Field) bool {
	return f.NthChild == 0 && f.Parent.Parent != nil && f.Optional()
}

func writeCases(f fields.Field) []int {
	return nil
}

func nilField(i int, f fields.Field) string {
	var flds []string
	var count int
	for j, o := range f.RepetitionTypes() {
		flds = append(flds, f.FieldNames()[j])
		if o == fields.Optional {
			count++
		}
		if count > i {
			break
		}
	}
	return strings.Join(flds, ".")
}
