package dremel

import (
	"github.com/parsyl/parquet/internal/parse"
)

var (
	writeRepTpl = `func {{.Func}}(x *Document, vals []{{.TypeName}}, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	defs, reps, nLevels = getDocLevels(defs, reps)

	{{if .Seen gt 0}}ind := indices(make([]int, {{.Seen}})){{end}}
	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		{{if .Seen gt 0}}ind.rep(rep){{end}}
		switch def { {{range $index, $case := .Cases}}
			case {{plus $case 1}}:
				{{.case}}{{end}}
		}
	}

	return nVals, nLevels
}`
)

func writeRepeated(f parse.Field) string {
	return ""
}
