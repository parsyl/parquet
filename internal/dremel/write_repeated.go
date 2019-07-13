package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/parse"
	"github.com/parsyl/parquet/internal/structs"
)

func writeRepeated(i int, f parse.Field, fields []parse.Field) string {

	return fmt.Sprintf(`func write%s(x *%s, vals []%s, defs, reps []uint8) (int, int) {
	l := findLevel(reps[1:], 0) + 1
	defs = defs[:l]
	reps = reps[:l]

	var v int%s
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		%s

		%s
	}

	return v, l
}`,
		strings.Join(f.FieldNames, ""),
		f.Type,
		f.TypeName,
		//indices(f, getUnseenRepeated(i, f, fields)),
		"",
		writeRepeatedIndices(f),
		writeRepeatedCases(f, getUnseenOptional(i, f, fields)),
	)
}

func writeRepeatedIndices(f parse.Field) string {
	return ""
}

func writeRepeatedCases(f parse.Field, unseen int) string {
	var out string
	for _, def := range f.Defs() {
		ch := f.Child(defIndex(def, f))
		fmt.Printf("for ch: %+v\n", ch)
		out += fmt.Sprintf(`case %d:
	%s
	`, def, structs.Init(def, f))
	}
	return fmt.Sprintf(`switch def {
	%s
}`, out)

}

func indices(f parse.Field, unseen int) string {
	if unseen == 0 {
		return ""
	}

	return fmt.Sprintf("\nindices := make([]int, %d)\n", unseen)
}

func getUnseenOptional(i int, f parse.Field, fields []parse.Field) int {
	return getUnseen(i, f, fields, parse.Optional)
}

func getUnseenRepeated(i int, f parse.Field, fields []parse.Field) int {
	return getUnseen(i, f, fields, parse.Repeated)
}

func getUnseen(i int, f parse.Field, fields []parse.Field, rt parse.RepetitionType) int {
	var seen []bool
	names := f.FieldNames

	for _, ff := range fields[:i] {
		newNames := ff.FieldNames
		var b []bool
		for i := range names {
			if f.RepetitionTypes[i] == rt && names[i] == newNames[i] {
				b = append(b, true)
			}
		}
		if len(b) > len(seen) {
			seen = b
		}
	}

	return nLevels(f, rt) - len(seen)
}

func nReps(f parse.Field) int {
	var n int
	for _, rt := range f.RepetitionTypes {
		if rt == parse.Repeated {
			n++
		}
	}
	return n
}

func nDefs(f parse.Field) int {
	var n int
	for _, rt := range f.RepetitionTypes {
		if rt == parse.Repeated || rt == parse.Optional {
			n++
		}
	}
	return n
}

func nLevels(f parse.Field, rt parse.RepetitionType) int {
	var n int
	for _, rt := range f.RepetitionTypes {
		if rt == rt {
			n++
		}
	}
	return n
}
