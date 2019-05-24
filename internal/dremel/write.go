package dremel

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/parse"
)

func init() {
	var err error
	writeTpl, err = template.New("output").Parse(`func write{{.FuncName}}(x *{{.Type}}, v {{.TypeName}}, def int64) {
	switch def { {{range .Cases}}
	{{.}}{{end}}
	}
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
	parse.Field
	Cases    []string
	FuncName string
}

func writeNested(f parse.Field) string {
	i := writeInput{
		Field:    f,
		FuncName: strings.Join(f.FieldNames, ""),
		Cases:    writeCases(f),
	}

	var buf bytes.Buffer
	err := writeTpl.Execute(&buf, i)
	if err != nil {
		log.Fatal(err) //TODO: return error
	}
	return string(buf.Bytes())
}

func writeCases(f parse.Field) []string {
	var out []string
	for i, o := range f.Optionals {
		if !o {
			continue
		}
		out = append(out, fmt.Sprintf(`case %d:
		%s`,
			i+1,
			strings.Join(inits(i, f), "\n"),
		))
	}
	return out
}

func inits(i int, f parse.Field) []string {
	var out []string
	for j, o := range f.Optionals[:i+1] {
		if !o {
			continue
		}
		out = append(out, initStruct(i, j+1, f))
	}
	return out
}

func initStruct(i, j int, f parse.Field) string {
	return ""
}

func writeRequired(f parse.Field) string {
	return fmt.Sprintf(`func write%s(x *%s, v %s, def int64) {
	x.%s = v
}`, strings.Join(f.FieldNames, ""), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
}

func pointer(p string, optional bool) string {
	if !optional {
		return ""
	}
	return p
}
