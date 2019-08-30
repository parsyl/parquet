package fields

import (
	"log"
	"text/template"
)

var (
	fieldTpl *template.Template
	initTpl  *template.Template
)

func init() {
	var err error
	fieldTpl, err = template.New("field").Parse(`x.{{.Parent}} = {{if .Append}}append(x.{{.Parent}}, {{.Val}}){{else}}{{.Val}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}

	initTpl = template.New("subfield")
	for _, t := range []string{
		`{{if eq .RT 0}}{{template "required" .}}{{else}}{{if eq .RT 1}}{{template "optional" .}}{{else}}{{template "repeated" .}}{{end}}{{end}}`,
		`{{define "required"}}{{if .Primitive}}{{.Val}}{{else}}{{.Type}}{ {{.Val}} }{{end}}{{end}}`,
		`{{define "optional"}}{{if .Primitive}}p{{.Type}}({{.Val}}){{else}}&{{.Type}}{ {{.Val}} }{{end}}{{end}}`,
		`{{define "repeated"}}{{if .Slice}}{{template "slice" .}}{{else}}{{if .Primitive}}{{.Val}}{{else}}{{.Type}}{ {{.Val}} } {{end}}{{end}}{{end}}`,
		`{{define "slice"}}{{if .Primitive}}[]{{.Type}}{ {{.Val}} }{{else}}[]{{.Type}}{ { {{.Val}} } }{{end}}{{end}}`,
	} {
		initTpl, err = initTpl.Parse(t)
		if err != nil {
			log.Fatal(err)
		}
	}
}
