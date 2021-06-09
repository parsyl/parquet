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
	writeTpl, err = template.New("output").Funcs(funcs).Parse(`func write{{.FuncName}}(x *{{.Field.StructType}}, vals []{{removeStar .Field.TypeName}}, defs, reps []uint8) (int, int) {
	def := defs[0]
	switch def { {{range $i, $case := .Cases}}
	case {{$case.Def}}:
	{{$case.Val}}{{if $case.MaxDef}}
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

// todo:  rename to defCase
type ifElses struct {
	Def    int
	MaxDef bool
	If     ifElse
	ElseIf []ifElse
	Else   *ifElse
	Val    *string
}

func (i ifElses) UseIf() bool {
	return i.Val == nil
}

func writeOptional(f fields.Field) string {
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
	md := f.MaxDef()
	for def := 1; def <= md; def++ {
		if f.NthChild == 0 || def == md {
			s := f.Init(def, 0)
			out = append(out, ifElses{Def: def, Val: &s, MaxDef: def == md})
		}
	}
	return out
}

// ifelses returns an if else block for the given definition and repetition level
func ifelses(def, rep int, fld fields.Field) ifElses {
	var flds []fields.Field
	for _, f := range fld.Chain() {
		if f.Optional() && f.NthChild == 0 {
			flds = append(flds, f)
		}
	}

	out := ifElses{
		If: ifElse{
			Cond: fmt.Sprintf("x.%s == nil", strings.Join(flds[0].FieldNames(), ".")),
			Val:  flds[0].Init(def, rep),
		},
	}

	if len(flds) > 1 {
		out.Else = &ifElse{
			Val: flds[len(flds)-1].Init(def, rep),
		}
	}

	if len(flds) > 2 {
		for _, f := range flds[1 : len(flds)-2] {
			out.ElseIf = append(out.ElseIf, ifElse{
				Cond: fmt.Sprintf("x.%s == nil", strings.Join(f.FieldNames(), ".")),
				Val:  f.Init(def, rep),
			})
		}
	}

	return out
}

// optionals returns a slice that contains the index of
// each optional field.
func optionals(def int, f fields.Field) []int {
	var out []int
	// di := f.DefIndex(def)
	// seen := append(f.Seen[:0:0], f.Seen...)

	// if len(seen) > di+1 {
	// 	seen = seen[:di+1]
	// }

	// for i, rt := range f.RepetitionTypes[:di+1] {
	// 	if rt >= fields.Optional {
	// 		out = append(out, i)
	// 	}
	// 	if i > len(seen)-1 && rt >= fields.Optional {
	// 		break
	// 	}
	// }

	return out
}
