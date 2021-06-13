package gen

import (
	"bytes"
	"fmt"
	"go/format"
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
func FromStruct(pth, outPth, typ, pkg, imp string, ignore bool) error {
	result, err := parse.Fields(typ, pth)
	if err != nil {
		return err
	}

	if len(result.Errors) > 0 && !ignore {
		return fmt.Errorf("not generating parquet.go (-ignore set to false), err: %v", result.Errors)
	}

	i := input{
		Package: pkg,
		Type:    typ,
		Import:  getImport(imp),
		Parent:  result.Parent,
	}

	tmpl := template.New("output").Funcs(funcs)
	tmpl, err = tmpl.Parse(tpl)
	if err != nil {
		return err
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
			return err
		}
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, i)
	if err != nil {
		return err
	}

	gocode, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("err: %s, gocode: %s", err, string(buf.Bytes()))
	}

	f, err := os.Create(outPth)
	if err != nil {
		return err
	}

	_, err = f.Write(gocode)
	if err != nil {
		return err
	}

	return f.Close()
}

// FromParquet generates a go struct, a reader, and a writer based
// on the parquet file at 'parq'
func FromParquet(parq, pth, outPth, typ, pkg, imp string, ignore bool) error {
	pf, err := os.Open(parq)
	if err != nil {
		return err
	}

	footer, err := parquet.ReadMetaData(pf)
	if err != nil {
		return fmt.Errorf("couldn't read footer: %s", err)
	}

	pf.Close()

	tmpl := template.New("output").Funcs(funcs)
	tmpl, err = tmpl.Parse(structTpl)
	if err != nil {
		return err
	}

	n := newStruct{
		Package: pkg,
		Structs: structs.Struct(typ, footer.Schema),
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, n)
	if err != nil {
		return err
	}

	gocode, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}

	f, err := os.Create(pth)
	if err != nil {
		return err
	}

	_, err = f.Write(gocode)
	if err != nil {
		return err
	}

	f.Close()
	return FromStruct(pth, outPth, typ, pkg, imp, ignore)
}

type input struct {
	Package string
	Type    string
	Import  string
	Parent  fields.Field
}

func getFieldType(se *sch.SchemaElement) (string, error) {
	if se.Type == nil {
		return "", fmt.Errorf("nil parquet schema type")
	}
	s := se.Type.String()
	out, ok := parquetTypes[s]
	if !ok {
		return "", fmt.Errorf("unsupported parquet schema type: %s", s)
	}

	if se.RepetitionType != nil && *se.RepetitionType == sch.FieldRepetitionType_REPEATED {
		return "", fmt.Errorf("field %s is FieldRepetitionType_REPEATED, which is currently not supported", se.Name)
	}

	var star string
	if se.RepetitionType != nil && *se.RepetitionType == sch.FieldRepetitionType_OPTIONAL {
		star = "*"
	}
	return fmt.Sprintf("%s%s", star, out), nil
}

func dedupe(flds []fields.Field) []fields.Field {
	fmt.Printf("deduping before: %+v\n", flds)
	seen := map[string]bool{}
	out := make([]fields.Field, 0, len(flds))
	for _, f := range flds {
		_, ok := seen[f.Category()]
		if !ok {
			out = append(out, f)
			seen[f.Category()] = true
		}
	}
	fmt.Println("deduping", out)

	for _, f := range out {
		fmt.Println("cat", f.Category())
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
