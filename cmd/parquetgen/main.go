package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/parsyl/parquet"
	"github.com/parsyl/parquet/internal/cases"
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
		"camelcase": func(s string) string {
			return cases.Camel(s)
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
		"compressionFunc": func(f parse.Field) string {
			if strings.Contains(f.FieldType, "Optional") {
				return "optionalFieldCompression"
			}
			return "fieldCompression"
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
	f.Close()
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
