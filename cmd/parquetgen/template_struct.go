package main

import "fmt"

var structTpl = `package {{.Package}}

// This code is generated by github.com/parsyl/parquet.

type {{.StructName}} struct { {{range .Fields}}
	{{template "structField" .}}{{end}}
}`

var structFieldTpl = fmt.Sprintf(`{{define "structField"}}{{titlecase .Name}} {{.Type}} %s:"{{.Name}}"%s{{end}}`, "`parquet", "`") // darn, can't escape backticks
