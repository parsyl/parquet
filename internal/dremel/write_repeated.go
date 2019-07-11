package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/parse"
)

func writeRepeated(i int, f parse.Field, fields []parse.Field) string {
	var seen []bool
	for i, ff := range fields[:i] {
		//if SOMETHING??? {
		seen = getSeen(seen, f.FieldNames, ff.FieldNames)
		//}
	}

	return fmt.Sprintf(`func write%s(x *%s, vals []string, defs, reps []uint8) (int, int) {
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
		indices(f, nReps(f), len(seen)),
		writeRepeatedIndices(f),
		writeRepeatedCases(f),
	)
}

func getSeen(seen []bool, names, newNames []string) []bool {
	var out []bool
	for i := range names {
		if names[i] == newNames[i] {
			out = append(out, true)
		}
	}

	if len(out) > len(seen) {
		return out
	}
	return seen
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

	return fmt.Sprintf("\nindices := make([]int, %d)\n", unseen)
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
