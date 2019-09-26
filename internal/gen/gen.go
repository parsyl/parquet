package gen

import (
	"bytes"
	"fmt"
	"go/format"
	"log"
	"os"
	"text/template"

	"github.com/parsyl/parquet"
	"github.com/parsyl/parquet/internal/fields"
	"github.com/parsyl/parquet/internal/parse"
	"github.com/parsyl/parquet/internal/structs"
	sch "github.com/parsyl/parquet/schema"
)

var (
	parquetTypes = map[string]string{
		"BOOLEAN":    "bool",
		"INT32":      "int32",
		"INT64":      "int64",
		"FLOAT":      "float32",
		"DOUBLE":     "float64",
		"BYTE_ARRAY": "string",
	}
)

// FromStruct generates a parquet reader and writer based on the struct
// of type 'typ' that is defined in the go file at 'pth'.
func FromStruct(pth, outPth, typ, pkg, imp string, ignore bool) {
	result, err := parse.Fields(typ, pth)
	if err != nil {
		log.Fatal(err)
	}

	for _, err := range result.Errors {
		log.Println(err)
	}

	if len(result.Errors) > 0 && !ignore {
		log.Fatal("not generating parquet.go (-ignore set to false), err: ", result.Errors)
	}

	i := input{
		Package: pkg,
		Type:    typ,
		Import:  getImport(imp),
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

	gocode, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(outPth)
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.Write(gocode)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()
}

// FromParquet generates a go struct, a reader, and a writer based
// on the parquet file at 'parq'
func FromParquet(parq, pth, outPth, typ, pkg, imp string, ignore bool) {
	pf, err := os.Open(parq)
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
		Package: pkg,
		Structs: structs.Struct(typ, footer.Schema),
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

	f, err := os.Create(pth)
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.Write(gocode)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()
	FromStruct(pth, outPth, typ, pkg, imp, ignore)
}

type input struct {
	Package string
	Type    string
	Import  string
	Fields  []fields.Field
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

func dedupe(flds []fields.Field) []fields.Field {
	seen := map[string]bool{}
	out := make([]fields.Field, 0, len(flds))
	for _, f := range flds {
		_, ok := seen[f.FieldType]
		if !ok {
			out = append(out, f)
			seen[f.FieldType] = true
		}
	}
	return out
}

func getImport(i string) string {
	if i == "" {
		return ""
	}
	return fmt.Sprintf(`"%s"`, i)
}

type newStruct struct {
	Package string
	Structs string
	Fields  []fields.Field
}

type fieldType struct {
	name string
	tpl  string
}
