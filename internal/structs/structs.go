package structs

import (
	"fmt"

	"github.com/parsyl/parquet/internal/parse"
)

// Init initializes the nested structs according to the
// current definition level.
func Init(def int, f parse.Field) string {
	return doInit(def, 0, f)
}

func doInit(def, i int, f parse.Field) string {
	var j int
	for _, o := range f.RepetitionTypes[:i+1] {
		if o == parse.Optional {
			j++
		}
	}

	if def == nDefs(f) && i == len(f.RepetitionTypes)-1 {
		var ptr string
		if f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Optional {
			ptr = "&"
		}
		return fmt.Sprintf("%s: %sv", f.FieldNames[i], ptr)
	}

	if i == def && def < nDefs(f) {
		return ""
	}

	var field string
	if i > 0 && i < len(f.RepetitionTypes) {
		field = fmt.Sprintf("%s: ", f.FieldNames[i])
	}

	var typ string
	var ptr string
	if i < nDefs(f) {
		typ = f.FieldTypes[i]
		if f.RepetitionTypes[i] == parse.Optional {
			ptr = "&"
		}
	}

	return fmt.Sprintf("%s%s%s{%s}", field, ptr, typ, doInit(def, i+1, f))
}

func nDefs(f parse.Field) int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == parse.Optional {
			out++
		}
	}
	return out
}
