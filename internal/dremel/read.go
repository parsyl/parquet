package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/parse"
)

var (
	readRepeatedTemplate = `if len({{.Field}}) == 0 {
		{{if gt 0 .I}}if i0 > 0 {
				lastRep = {{.Rep}}
			}{{end}}
		defs = append(defs, {{.Def}})
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			
		}
	}`
)

func readRequired(f parse.Field) string {
	return fmt.Sprintf(`func read%s(x %s) %s {
	return x.%s
}`, strings.Join(f.FieldNames, ""), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
}

func readOptional(f parse.Field) string {
	var out string
	n := defs(f)
	for def := 0; def < n; def++ {
		out += fmt.Sprintf(`case x.%s == nil:
		return nil, []uint8{%d}, nil
`, nilField(def, f), def)
	}

	var ptr string
	if f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Optional {
		ptr = "*"
	}
	out += fmt.Sprintf(`	default:
		return []%s{%sx.%s}, []uint8{%d}, nil`, strings.Replace(f.TypeName, "*", "", 1), ptr, nilField(n, f), n)

	if f.RepetitionTypes[len(f.RepetitionTypes)-1] == parse.Required {
		ptr = "*"
	}

	return fmt.Sprintf(`func read%s(x %s) ([]%s, []uint8, []uint8) {
	switch {
	%s
	}
}`, strings.Join(f.FieldNames, ""), f.Type, strings.Replace(f.TypeName, "*", "", 1), out)
}

func readRepeated(f parse.Field) string {
	return fmt.Sprintf(`func read%s(x %s) ([]%s, []uint8, []uint8) {
	var vals []%s
	var defs, reps []uint8
	var lastRep uint8

	%s

	return vals, defs, reps	
}`,
		strings.Join(f.FieldNames, ""),
		f.Type,
		strings.Replace(f.TypeName, "*", "", 1),
		strings.Replace(f.TypeName, "*", "", 1),
		doReadRepeated(f, 0),
	)
}

func doReadRepeated(f parse.Field, depth int) string {
	if depth == len(f.RepetitionTypes) {
		fn := nilField(depth-1, f)
		return fmt.Sprintf(`vals = append(vals, x.%s)`, fn)
	}

	fn := nilField(depth, f)

	var ifStmt string
	if f.RepetitionTypes[depth] == parse.Optional {
		ifStmt = fmt.Sprintf("x.%s == nil", fn)
	} else if f.RepetitionTypes[depth] == parse.Repeated {
		ifStmt = fmt.Sprintf("len(x.%s) == 0", fn)
	}

	var lastRep string
	lastRep = fmt.Sprintf(`if i%d > 0 {
lastRep = %d
}
`, depth, depth+1)

	return fmt.Sprintf(`if %s {
		defs = append(defs, %d)
		reps = append(reps, %d)
	} else {
		for i%d, x%d := range x.%s {
			%sdefs = append(defs, %d)
			reps = append(reps, lastRep)
		%s
		
}
}`, ifStmt, depth, depth, depth, depth, fn, lastRep, depth+1, doReadRepeated(f, depth+1))
}
