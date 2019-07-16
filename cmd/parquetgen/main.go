package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/parsyl/parquet"
	"github.com/parsyl/parquet/internal/cases"
	"github.com/parsyl/parquet/internal/dremel"
	"github.com/parsyl/parquet/internal/parse"
	"github.com/parsyl/parquet/internal/structs"
	sch "github.com/parsyl/parquet/schema"
)

var (
	metadata     = flag.Bool("metadata", false, "print the metadata of a parquet file (-parquet) and exit")
	pageheaders  = flag.Bool("pageheaders", false, "print the page headers of a parquet file (-parquet) and exit (also prints the metadata)")
	typ          = flag.String("type", "", "name of the struct that will used for writing and reading")
	pkg          = flag.String("package", "", "package of the generated code")
	imp          = flag.String("import", "", "import statement of -type if it doesn't live in -package")
	pth          = flag.String("input", "", "path to the go file that defines -type")
	outPth       = flag.String("output", "parquet.go", "name of the file that is produced, defaults to parquet.go")
	ignore       = flag.Bool("ignore", true, "ignore unsupported fields in -type, otherwise log.Fatal is called when an unsupported type is encountered")
	parq         = flag.String("parquet", "", "path to a parquet file (if you are generating code based on an existing parquet file or printing the file metadata or page headers)")
	structOutPth = flag.String("struct-output", "generated_struct.go", "name of the file that is produced, defaults to parquet.go")
)

func main() {
	flag.Parse()

	if *pth != "" && *parq != "" {
		log.Fatal("choose -parquet or -input, but not both")
	}

	if *metadata {
		readFooter()
	} else if *pageheaders {
		readPageHeaders()
	} else if *parq == "" {
		fromStruct(*pth)
	} else {
		fromParquet()
	}
}

var (
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
			return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
		},
		"camelCase": func(s string) string {
			return cases.Camel(s)
		},
		"camelCaseRemoveStar": func(s string) string {
			return cases.Camel(strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1))
		},
		"dedupe": dedupe,
		"compressionFunc": func(f parse.Field) string {
			if strings.Contains(f.FieldType, "Optional") {
				return "optionalFieldCompression"
			}
			return "fieldCompression"
		},
		"funcName": func(f parse.Field) string {
			return strings.Join(f.FieldNames, "")
		},
		"join": func(names []string) string {
			return strings.Join(names, ".")
		},
		"imports": func(fields []parse.Field) []string {
			var out []string
			var intFound, stringFound bool
			for _, f := range fields {
				if !intFound && strings.Contains(f.TypeName, "int") {
					intFound = true
					out = append(out, `"math"`)
				}
				if !stringFound && strings.Contains(f.TypeName, "string") {
					stringFound = true
					out = append(out, `"sort"`)
				}
			}
			return out
		},
		"maxType": func(f parse.Field) string {
			var out string
			switch f.TypeName {
			case "int32", "*int32":
				out = "math.MaxInt32"
			case "int64", "*int64":
				out = "math.MaxInt64"
			case "uint32", "*uint32":
				out = "math.MaxUint32"
			case "uint64", "*uint64":
				out = "math.MaxUint64"
			case "float32", "*float32":
				out = "math.MaxFloat32"
			case "float64", "*float64":
				out = "math.MaxFloat64"
			}
			return out
		},
		"columnName":    func(f parse.Field) string { return strings.ToLower(strings.Join(f.FieldNames, ".")) },
		"writeFunc":     dremel.Write,
		"readFunc":      dremel.Read,
		"writeFuncName": func(f parse.Field) string { return fmt.Sprintf("write%s", strings.Join(f.FieldNames, "")) },
		"readFuncName":  func(f parse.Field) string { return fmt.Sprintf("read%s", strings.Join(f.FieldNames, "")) },
		"parquetType": func(f parse.Field) string {
			if f.Optional() {
				return "parquet.OptionalField"
			}
			return "parquet.RequiredField"
		},
	}
)

func templateDict(values ...interface{}) (map[string]interface{}, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("invalid dict call")
	}
	dict := make(map[string]interface{}, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dict keys must be strings")
		}
		dict[key] = values[i+1]
	}
	return dict, nil
}

type newStruct struct {
	Package string
	Structs string
	Fields  []parse.Field
}

type fieldType struct {
	name string
	tpl  string
}

func readPageHeaders() {
	f := openParquet()
	footer := getFooter(f)

	pageHeaders, err := parquet.PageHeaders(footer, f)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(struct {
		PageHeaders []sch.PageHeader `json:"page_headers"`
		MetaData    sch.FileMetaData `json:"file_metadata"`
	}{
		PageHeaders: pageHeaders,
		MetaData:    *footer,
	})
}

func readFooter() {
	f := openParquet()
	footer := getFooter(f)
	f.Close()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(footer)
}

func openParquet() *os.File {
	if *parq == "" {
		log.Fatal("-parquet is required with -footer")
	}

	f, err := os.Open(*parq)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func getFooter(r io.ReadSeeker) *sch.FileMetaData {
	footer, err := parquet.ReadMetaData(r)
	if err != nil {
		log.Fatal("couldn't read footer: ", err)
	}
	return footer
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

	n := newStruct{
		Package: *pkg,
		Structs: structs.Struct(*typ, footer.Schema),
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
		requiredNumericTpl,
		optionalNumericTpl,
		stringTpl,
		stringOptionalTpl,
		boolTpl,
		boolOptionalTpl,
		newFieldTpl,
		requiredStatsTpl,
		optionalStatsTpl,
		boolStatsTpl,
		boolOptionalStatsTpl,
		stringStatsTpl,
		stringOptionalStatsTpl,
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

	// gocode, err := format.Source(buf.Bytes())
	// if err != nil {
	// 	log.Fatal(err)
	// }

	f, err := os.Create(*outPth)
	if err != nil {
		log.Fatal(err)
	}

	//_, err = f.Write(gocode)
	_, err = f.Write(buf.Bytes())
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
		log.Fatalf("unsupported parquet schema type: %s", s)
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

func dedupe(fields []parse.Field) []parse.Field {
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
}
