package structs

import (
	"fmt"

	"github.com/parsyl/parquet/internal/parse"
)

func Init(def int, f parse.Field) string {
	return doInit(def, 0, f)
}

func doInit(def, i int, f parse.Field) string {
	var j int
	for _, o := range f.Optionals[:i+1] {
		if o {
			j++
		}
	}

	if j == def && j == nDefs(f) {
		var ptr string
		if f.Optionals[i] {
			ptr = "*"
		}
		return fmt.Sprintf("%s: %sv", f.FieldNames[i], ptr)
	}

	if i == def {
		return ""
	}

	var field string
	if i > 0 && j < nDefs(f) {
		field = fmt.Sprintf("%s: ", f.FieldNames[i])
	}

	var typ string
	var ptr string
	if i < nDefs(f) {
		typ = f.FieldTypes[i]
		if f.Optionals[i] {
			ptr = "&"
		}
	}

	return fmt.Sprintf("%s%s%s{%s}", field, ptr, typ, doInit(def, i+1, f))
}

func nDefs(f parse.Field) int {
	var out int
	for _, o := range f.Optionals {
		if o {
			out++
		}
	}
	return out
}
