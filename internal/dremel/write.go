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
	for i := range f.Optionals {
		if !validCase(i, f.Optionals) {
			continue
		}
		out = append(out, fmt.Sprintf(`case %d:
		%s`,
			i+1,
			strings.Join(inits(i, f), " "),
		))
	}
	return out
}

func validCase(i int, optionals []bool) bool {
	if i == len(optionals)-1 {
		return true
	}
	return optionals[i] && allOptional(optionals[i+1:])
}

func allOptional(b []bool) bool {
	for _, x := range b {
		if !x {
			return false
		}
	}
	return true
}

func inits(i int, f parse.Field) []string {
	var out []string
	for j := range f.Optionals[:i+1] {
		if !validCase(i, f.Optionals) {
			continue
		}
		out = append(out, fmt.Sprintf(`%s {
			x.%s = %s
		}`,
			ifelse(j, f),
			strings.Join(f.FieldNames[:j+1], "."),
			initStruct(i, j, f)))
	}
	return out
}

func ifelse(i int, f parse.Field) string {
	switch {
	case i == 0:
		return fmt.Sprintf("if x.%s == nil", strings.Join(f.FieldNames[:i+1], "."))
	case i+1 == len(f.FieldNames):
		return "else"
	default:
		return fmt.Sprintf("else if x.%s == nil", strings.Join(f.FieldNames[:i+1], "."))
	}
}

func initStruct(cs, i int, f parse.Field) string {
	switch {
	case cs == 0 && i == 0:
		return fmt.Sprintf("&%s{}", f.FieldNames[0])
	case i < cs:
		return doInit(cs, i, f.Optionals, f.FieldNames)
	default:
		p := ""
		if !f.Optionals[i] {
			p = "*"
		}
		return fmt.Sprintf("%sv", p)
	}
}

func doInit(i, n int, levels []bool, names []string) string {
	if n == len(levels) {
		p := ""
		if !levels[i] {
			p = "*"
		}
		return fmt.Sprintf(": %sv", p)
	}
	brackets := []string{"{", "}"}
	if n == len(levels)-1 {
		brackets = []string{"", ""}
	}
	return fmt.Sprintf(
		`%s%s%s%s%s`,
		pointer(i, n, "&", levels),
		names[n],
		brackets[0],
		doInit(i, n+1, levels, names),
		brackets[1],
	)
}

func writeRequired(f parse.Field) string {
	return fmt.Sprintf(`func write%s(x *%s, v %s, def int64) {
	x.%s = v
}`, strings.Join(f.FieldNames, ""), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
}

func pointer(i, n int, p string, levels []bool) string {
	if levels[n] && n < i {
		return p
	}
	return ""
}
