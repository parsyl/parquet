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

func writeRequired(f parse.Field) string {
	return fmt.Sprintf(`func write%s(x *%s, v %s, def int64) {
	x.%s = v
}`, strings.Join(f.FieldNames, ""), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
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
	for def := 1; def <= defs(f); def++ {
		out = append(out, fmt.Sprintf(`case %d:
	%s`, def, ifelse(0, def, f)))
	}
	return out
}

// return an if else block for the definition level
func ifelse(i, def int, f parse.Field) string {
	if i == def {
		return ""
	}

	stmt := "if"
	brace := "}"
	cmp := fmt.Sprintf(" x.%s == nil", nilField(i, f))
	if i == defs(f)-1 {
		stmt = " else"
		cmp = ""
		brace = ""
	} else if i > 0 && i < defs(f)-1 {
		stmt = " else if"
		brace = ""
	}

	return fmt.Sprintf(`%s%s {
	%s = %s
	%s%s`, stmt, cmp, "x", structs.Init(def, f), brace, ifelse(i+1, def, f))
}

func nilField(i int, f parse.Field) string {
	var fields []string
	var count int
	for i, o := range f.Optionals {
		fields = append(fields, f.FieldNames[i])
		if o {
			count++
		}
		if count > i {
			break
		}
	}
	return strings.Join(fields, ".")
}

// count the number of fields in the path that can be optional
func defs(f parse.Field) int {
	var out int
	for _, o := range f.Optionals {
		if o {
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
