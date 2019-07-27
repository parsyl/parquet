package gen

import (
	"fmt"
	"html/template"
	"strings"

	"github.com/parsyl/parquet/internal/cases"
	"github.com/parsyl/parquet/internal/dremel"
	"github.com/parsyl/parquet/internal/parse"
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
