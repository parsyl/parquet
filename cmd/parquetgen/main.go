package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/parsyl/parquet"
	sch "github.com/parsyl/parquet/generated"
	"github.com/parsyl/parquet/internal/parse"
)

var (
	metadata     = flag.Bool("metadata", false, "print the metadata of a parquet file (-parquet) and exit")
	typ          = flag.String("type", "", "name of the struct that will used for writing and reading")
	pkg          = flag.String("package", "", "package of the generated code")
	imp          = flag.String("import", "", "import statement of -type if it doesn't live in -package")
	pth          = flag.String("input", "", "path to the go file that defines -type")
	outPth       = flag.String("output", "parquet.go", "name of the file that is produced, defaults to parquet.go")
	ignore       = flag.Bool("ignore", true, "ignore unsupported fields in -type, otherwise log.Fatal is called when an unsupported type is encountered")
	parq         = flag.String("parquet", "", "path to a parquet file (if you are generating code based on an existing parquet file)")
	structOutPth = flag.String("struct-output", "generated_struct.go", "name of the file that is produced, defaults to parquet.go")

	parquetTypes = map[string]string{
		"BOOLEAN":    "bool",
		"INT32":      "int32",
		"INT64":      "int64",
		"FLOAT":      "float32",
		"DOUBLE":     "float64",
		"BYTE_ARRAY": "string",
	}

	funcs = template.FuncMap{
		"removeStar": func(s string) string {
			return strings.Replace(s, "*", "", 1)
		},
		"titlecase": func(s string) string {
			return strings.Title(s)
		},
		"dedupe": func(fields []parse.Field) []parse.Field {
			seen := map[string]bool{}
			out := make([]parse.Field, 0, len(fields))
			for _, f := range fields {
				_, ok := seen[f.FieldType]
				if !ok {
					out = append(out, f)
					seen[f.FieldType] = true
				}
			}
			return out
		},
	}
)

type structField struct {
	Name string
	Type string
}

type newStruct struct {
	Package    string
	StructName string
	Fields     []structField
}

type fieldType struct {
	name string
	tpl  string
}

func main() {
	flag.Parse()

	if *pth != "" && *parq != "" {
		log.Fatal("choose -parquet or -input, but not both")
	}

	if *metadata {
		readFooter()
	} else if *parq == "" {
		fromStruct(*pth)
	} else {
		fromParquet()
	}
}

func readFooter() {
	if *parq == "" {
		log.Fatal("-parquet is required with -footer")
	}

	pf, err := os.Open(*parq)
	if err != nil {
		log.Fatal(err)
	}

	footer, err := parquet.ReadMetaData(pf)
	if err != nil {
		log.Fatal("couldn't read footer: ", err)
	}

	pf.Close()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(footer)
}

func fromParquet() {
	pf, err := os.Open(*parq)
	if err != nil {
		log.Fatal(err)
	}

	footer, err := parquet.ReadMetaData(pf)
	if err != nil {
		log.Fatal("couldn't read footer: ", err)
	}

	pf.Close()

	tmpl := template.New("output").Funcs(funcs)
	tmpl, err = tmpl.Parse(structTpl)
	if err != nil {
		log.Fatal(err)
	}

	tmpl, err = tmpl.Parse(structFieldTpl)
	if err != nil {
		log.Fatal(err)
	}

	fields := make([]structField, len(footer.Schema[1:]))
	for i, s := range footer.Schema[1:] {
		fields[i] = structField{
			Name: s.Name,
			Type: getFieldType(s),
		}
	}
	n := newStruct{
		Package:    *pkg,
		StructName: *typ,
		Fields:     fields,
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, n)
	if err != nil {
		log.Fatal(err)
	}

	gocode, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(*structOutPth)
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.Write(gocode)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()
	fromStruct(*structOutPth)
}

func fromStruct(pth string) {
	result, err := parse.Fields(*typ, pth)
	if err != nil {
		log.Fatal(err)
	}

	for _, err := range result.Errors {
		log.Println(err)
	}

	if len(result.Errors) > 0 && !*ignore {
		log.Fatal("not generating parquet.go (-ignore set to false), err: ", result.Errors)
	}

	i := input{
		Package: *pkg,
		Type:    *typ,
		Import:  getImport(*imp),
		Fields:  result.Fields,
	}

	tmpl := template.New("output").Funcs(funcs)
	tmpl, err = tmpl.Parse(tpl)
	if err != nil {
		log.Fatal(err)
	}

	for _, t := range []string{
		requiredTpl,
		optionalTpl,
		stringTpl,
		stringOptionalTpl,
		boolTpl,
		boolOptionalTpl,
		newFieldTpl,
	} {
		var err error
		tmpl, err = tmpl.Parse(t)
		if err != nil {
			log.Fatal(err)
		}
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, i)
	if err != nil {
		log.Fatal(err)
	}

	gocode, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(*outPth)
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.Write(gocode)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()
}

func getFieldType(se *sch.SchemaElement) string {
	if se.Type == nil {
		log.Fatal("nil parquet schema type")
	}
	s := se.Type.String()
	out, ok := parquetTypes[s]
	if !ok {
		log.Fatal("unsupported parquet schema type: %s", s)
	}

	if se.RepetitionType != nil && *se.RepetitionType == sch.FieldRepetitionType_REPEATED {
		log.Fatalf("field %s is FieldRepetitionType_REPEATED, which is currently not supported", se.Name)
	}

	var star string
	if se.RepetitionType != nil && *se.RepetitionType == sch.FieldRepetitionType_OPTIONAL {
		star = "*"
	}
	return fmt.Sprintf("%s%s", star, out)
}

func getImport(i string) string {
	if i == "" {
		return ""
	}
	return fmt.Sprintf(`"%s"`, i)
}

type input struct {
	Package string
	Type    string
	Import  string
	Fields  []parse.Field
}
