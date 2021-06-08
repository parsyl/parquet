package dremel

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/fields"
)

func init() {
	funcs := template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(s, "*", "", 1)
		},
		"plusOne": func(i int) int { return i + 1 },
		"notNil":  func(x *ifElse) bool { return x != nil },
	}

	var err error
	writeTpl, err = template.New("output").Funcs(funcs).Parse(`func write{{.FuncName}}(x *{{.Field.Type}}, vals []{{removeStar .Field.TypeName}}, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def { {{range $i, $case := .Cases}}{{$def:=plusOne $i}}
	case {{$def}}:
	{{$defIndex := $.Field.DefIndex $def}}{{if $case.UseIf}}{{template "ifelse" $case}}{{else}}{{$case.Val}}{{end}}{{if eq $def $.MaxDef}}
	return 1, 1{{end}}{{end}}
	}

	return 0, 1
}`)
	if err != nil {
		log.Fatal(err)
	}

	ifelseStmt = `{{define "ifelse"}}if {{.If.Cond}} {
	{{.If.Val}}
} {{range $else := .ElseIf}} else if {{$else.Cond}} {
	{{$else.Val}}
}{{end}} {{if notNil .Else}} else {
	{{.Else.Val}}
} {{end}}{{end}}`

	writeTpl, err = writeTpl.Parse(ifelseStmt)
	if err != nil {
		log.Fatal(err)
	}
}

var (
	writeTpl   *template.Template
	ifelseStmt string
)

type writeInput struct {
	fields.Field
	Cases    []ifElses
	FuncName string
}

type ifElse struct {
	Cond string
	Val  string
}

type ifElses struct {
	If     ifElse
	ElseIf []ifElse
	Else   *ifElse
	Val    *string
}

func (i ifElses) UseIf() bool {
	return i.Val == nil
}

func writeOptional(i int, flds []fields.Field) string {
	f := flds[i]
	wi := writeInput{
		Field:    f,
		FuncName: strings.Join(f.FieldNames(), ""),
		Cases:    writeOptionalCases(f),
	}

	var buf bytes.Buffer
	err := writeTpl.Execute(&buf, wi)
	if err != nil {
		log.Fatal(err) //TODO: return error
	}
	return string(buf.Bytes())
}

func writeOptionalCases(f fields.Field) []ifElses {
	var out []ifElses
	for def := 1; def <= f.MaxDef(); def++ {
		if useIfElse(def, 0, f) {
			out = append(out, ifelses(def, 0, f))
		} else {
			s := f.Init(def, 0)
			out = append(out, ifElses{Val: &s})
		}
	}
	return out
}

type ifElseCase struct {
	f fields.Field
	p *fields.Field
}

// ifelses returns an if else block for the given definition and repetition level
func ifelses(def, rep int, f fields.Field) ifElses {
	opts := optionals(def, f)
	var cases ifElseCases
	for _, o := range opts {
		//f := orig.Copy()
		//f.Seen = seens(o)
		cases = append(cases, ifElseCase{f: f, p: f.Parent(o + 1)})
	}

	return cases.ifElses(def, rep, int(f.MaxDef()))
}

func seens(i int) fields.RepetitionTypes {
	out := make([]fields.RepetitionType, i)
	for i := range out {
		out[i] = fields.Repeated
	}
	return fields.RepetitionTypes(out)
}

type ifElseCases []ifElseCase

func (i ifElseCases) ifElses(def, rep, md int) ifElses {
	out := ifElses{
		If: ifElse{
			Cond: fmt.Sprintf("x.%s == nil", strings.Join(i[0].p.FieldNames, ".")),
			Val:  i[0].f.Init(def, rep),
		},
	}

	var leftovers []ifElseCase
	if len(i) > 1 {
		out.Else = &ifElse{
			Val: i[len(i)-1].f.Init(def, rep),
		}
		if len(i) > 2 {
			leftovers = i[1 : len(i)-1]
		}
	}

	for _, iec := range leftovers {
		out.ElseIf = append(out.ElseIf, ifElse{
			Cond: fmt.Sprintf("x.%s == nil", strings.Join(iec.p.FieldName, ".")),
			Val:  iec.f.Init(def, rep),
		})
	}

	return out
}

// optionals returns a slice that contains the index of
// each optional field.
func optionals(def int, f fields.Field) []int {
	var out []int
	di := f.DefIndex(def)
	seen := append(f.Seen[:0:0], f.Seen...)

	if len(seen) > di+1 {
		seen = seen[:di+1]
	}

	for i, rt := range f.RepetitionTypes[:di+1] {
		if rt >= fields.Optional {
			out = append(out, i)
		}
		if i > len(seen)-1 && rt >= fields.Optional {
			break
		}
	}

	return out
}
