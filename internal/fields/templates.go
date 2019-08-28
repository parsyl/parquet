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

	initTpl, err = template.New("subfield").Parse(`{{if eq .RT 0}}{{template "required" .}}{{else}}{{if eq .RT 1}}{{template "optional" .}}{{else}}{{template "repeated" .}}{{end}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}

	initTpl, err = initTpl.Parse(`{{define "required"}}{{if .Primitive}}{{.Val}}{{else}}{{.Type}}{ {{.Val}} }{{end}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}

	initTpl, err = initTpl.Parse(`{{define "optional"}}{{if .Primitive}}p{{.Type}}({{.Val}}){{else}}&{{.Type}}{ {{.Val}} }{{end}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}

	initTpl, err = initTpl.Parse(`{{define "repeated"}}{{if .Slice}}{{template "slice" .}}{{else}}{{if .Primitive}}{{.Val}}{{else}}{{.Type}}{ {{.Val}} } {{end}}{{end}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}

	initTpl, err = initTpl.Parse(`{{define "slice"}}{{if .Primitive}}[]{{.Type}}{ {{.Val}} }{{else}}[]{{.Type}}{ { {{.Val}} } }{{end}}{{end}}`)
	if err != nil {
		log.Fatal(err)
	}
}
