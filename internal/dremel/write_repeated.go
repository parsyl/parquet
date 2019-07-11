package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/parse"
)

func writeRepeated(f parse.Field, fields []parse.Field, i int) string {
	var seen int

	for _, ff := range fields[:i] {

	}

	return fmt.Sprintf(`func write%s(x *%s, vals []string, defs, reps []uint8) (int, int) {
	l := findLevel(reps[1:], 0) + 1
	defs = defs[:l]
	reps = reps[:l]

	var v int
	%s
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
		indices(f, seen),
		nReps(f),
		writeRepeatedIndices(f),
		writeRepeatedCases(f),
	)
}

func writeRepeatedIndices(f parse.Field) string {
	return ""
}

func writeRepeatedCases(f parse.Field) string {
	return ""
}

func indices(f parse.Field, seen int) string {
	if seen == 0 {
		return ""
	}

	return fmt.Sprintf("indices := make([]int, %d)", seen)
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
