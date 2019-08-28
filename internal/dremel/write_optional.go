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
	s := fields.Seen(i, flds)
	f.Seen = s
	wi := writeInput{
		Field:    f,
		FuncName: strings.Join(f.FieldNames, ""),
		Cases:    writeOptionalCases(f, s),
	}

	var buf bytes.Buffer
	err := writeTpl.Execute(&buf, wi)
	if err != nil {
		log.Fatal(err) //TODO: return error
	}
	return string(buf.Bytes())
}

func writeOptionalCases(f fields.Field, seen fields.RepetitionTypes) []ifElses {
	var out []ifElses
	for def := 1; def <= defs(f); def++ {
		if useIfElse(def, 0, seen, f) {
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
	p fields.Field
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
	if def == md {
		out.Else = &ifElse{
			Val: i[len(i)-1].f.Init(def, rep),
		}
		if len(i) > 1 {
			leftovers = i[1 : len(i)-1]
		}

	} else if len(i) > 1 {
		leftovers = i[1:]
	}

	for _, iec := range leftovers {
		out.ElseIf = append(out.ElseIf, ifElse{
			Cond: fmt.Sprintf("x.%s == nil", strings.Join(iec.p.FieldNames, ".")),
			Val:  iec.f.Init(def, rep),
		})
	}

	return out
}

// ifelses returns an if else block for the given definition level
func ifelses(def, rep int, orig fields.Field) ifElses {
	opts := optionals(def, orig)
	var seen []fields.RepetitionType

	di := int(orig.DefIndex(def))

	for _, rt := range orig.RepetitionTypes[:di+1] {
		if rt == fields.Required {
			seen = append(seen, fields.Repeated)
		} else {
			break
		}
	}

	var cases ifElseCases
	for _, o := range opts {
		f := orig.Copy()
		if len(orig.Seen) <= len(seen) {
			f.Seen = append(seen[:0:0], seen...)
		}
		cases = append(cases, ifElseCase{f: f, p: f.Parent(o + 1)})
		seen = append(seen, fields.Repeated)
	}

	return cases.ifElses(def, rep, int(orig.MaxDef()))
}

func optionals(def int, f fields.Field) []int {
	var out []int
	di := f.DefIndex(def)
	for i, rt := range f.RepetitionTypes[:di+1] {
		if rt == fields.Optional {
			out = append(out, i)
		}
	}

	if def == int(f.MaxDef()) && len(f.RepetitionTypes) > di+1 && f.RepetitionTypes[di+1] == fields.Required {
		out = append(out, out[len(out)-1])
	} else if def == int(f.MaxDef()) && len(out) == 1 {
		out = append(out, out[len(out)-1])
	}

	return out
}
