package dremel

import (
	"bytes"
	"log"
	"strings"
	"text/template"

	"github.com/inigolabs/parquet/cmd/parquetgen/fields"
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
	Cases    []defCases
	FuncName string
}

type ifElse struct {
	Cond string
	Val  string
}

type defCases struct {
	Def      int
	MaxDef   bool
	Val      *string
	RepCases []string
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

func writeOptionalCases(f fields.Field) []defCases {
	md := f.MaxDef()
	cases := writeCases(f)
	out := make([]defCases, len(cases))
	for i, def := range cases {
		s := f.Init(def, 0)
		out[i] = defCases{Def: def, Val: &s, MaxDef: def == md}
	}
	return out
}
