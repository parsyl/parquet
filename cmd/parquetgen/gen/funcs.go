package gen

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/cmd/parquetgen/cases"
	"github.com/parsyl/parquet/cmd/parquetgen/dremel"
	"github.com/parsyl/parquet/cmd/parquetgen/fields"
)

var (
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
		"compressionFunc": func(f fields.Field) string {
			if strings.Contains(f.Category(), "Optional") {
				return "optionalFieldCompression"
			}
			return "fieldCompression"
		},
		"funcName": func(f fields.Field) string {
			return strings.Join(f.FieldNames(), "")
		},
		"join": func(names []string) string {
			return strings.Join(names, ".")
		},
		"joinTypes": func(t []fields.RepetitionType) string {
			names := make([]string, len(t))
			for i, ty := range t {
				names[i] = fmt.Sprintf("%d", ty)
			}
			return strings.Join(names, ", ")
		},
		"imports": func(fields []fields.Field) []string {
			var out []string
			var intFound bool
			for _, f := range fields {
				if !intFound && strings.Contains(f.Type, "int") {
					intFound = true
					out = append(out, `"math"`)
				}
			}
			return out
		},
		"maxType": func(f fields.Field) string {
			var out string
			switch f.Type {
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
		"columnName":    func(f fields.Field) string { return strings.Join(f.ColumnNames(), ".") },
		"writeFunc":     dremel.Write,
		"readFunc":      dremel.Read,
		"writeFuncName": func(f fields.Field) string { return fmt.Sprintf("write%s", strings.Join(f.FieldNames(), "")) },
		"readFuncName":  func(f fields.Field) string { return fmt.Sprintf("read%s", strings.Join(f.FieldNames(), "")) },
		"parquetType": func(f fields.Field) string {
			if f.Optional() {
				return "parquet.OptionalField"
			}
			return "parquet.RequiredField"
		},
		"byteSize": func(f fields.Field) string {
			var out string
			switch f.Type {
			case "int32", "*int32", "uint32", "*uint32", "float32", "*float32":
				out = "4"
			case "int64", "*int64", "uint64", "*uint64", "float64", "*float64":
				out = "8"
			}
			return out
		},
		// based on binary.Write
		"putFunc": func(f fields.Field) string {
			var out string
			switch f.Type {
			case "int32", "*int32", "uint32", "*uint32", "float32", "*float32":
				out = "PutUint32"
			case "int64", "*int64", "uint64", "*uint64", "float64", "*float64":
				out = "PutUint64"
			}
			return out
		},
		// based on binary.Write
		"uintFunc": func(f fields.Field) string {
			var out string
			switch f.Type {
			case "int32":
				out = "uint32(v)"
			case "*int32":
				out = "uint32(*v)"
			case "uint32":
				out = "v"
			case "*uint32":
				out = "*v"
			case "float32":
				out = "math.Float32bits(v)"
			case "*float32":
				out = "math.Float32bits(*v)"
			case "int64":
				out = "uint64(v)"
			case "*int64":
				out = "uint64(*v)"
			case "uint64":
				out = "v"
			case "*uint64":
				out = "*v"
			case "float64":
				out = "math.Float64bits(v)"
			case "*float64":
				out = "math.Float64bits(*v)"
			}
			return out
		},
	}
)
