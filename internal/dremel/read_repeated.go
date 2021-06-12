package dremel

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/fields"
)

func init() {
	m := template.FuncMap{
		"inc": func(i int) int {
			return i + 1
		},
	}

	var err error
	readRepeatedRepeatedTpl, err = template.New("output").Funcs(m).Parse(readRepeatedRepeatedText)
	if err != nil {
		log.Fatalf("rrr: %s", err)
	}

	readRepeatedOptionalTpl, err = template.New("output").Parse(readRepeatedOptionalText)
	if err != nil {
		log.Fatalf("rro: %s", err)
	}
}

var (
	readRepeatedRepeatedTpl  *template.Template
	readRepeatedRepeatedText = `if len({{.Var}}.{{.Field}}) == 0 {
         defs = append(defs, {{.Def}})
 		 reps = append(reps, lastRep)
     } else {
         for i{{.Rep}}, x{{.Rep}} := range {{.Var}}.{{.Field}} {
             if i{{.Rep}} == 1 {
				lastRep = {{inc .Rep}}
			}
            %s
         }
     }`

	readRepeatedOptionalTpl  *template.Template
	readRepeatedOptionalText = `if {{.Var}}.{{.Field}} == nil {
		defs = append(defs, {{.Def}})
		reps = append(reps, lastRep)
	} else {
        %s
    }`
)

type readClause struct {
	Var   string
	Field string
	Def   int
	Rep   int
}

func readRepeated(f fields.Field) string {
	return fmt.Sprintf(`func read%s(x %s) ([]%s, []uint8, []uint8) {
	var vals []%s
	var defs, reps []uint8
	var lastRep uint8

	%s

	return vals, defs, reps	
}`,
		strings.Join(f.FieldNames(), ""),
		f.StructType(),
		cleanTypeName(f.TypeName),
		cleanTypeName(f.TypeName),
		doReadRepeated(f, 0, "x"),
	)
}

func doReadRepeated(f fields.Field, i int, varName string) string {
	if i == f.MaxDef() {
		rts := f.RepetitionTypes()
		if rts[len(rts)-1] == fields.Optional {
			varName = fmt.Sprintf("*%s", varName)
		}
		if rts[len(rts)-1] != fields.Repeated {
			n := lastRepeated(rts)
			varName = strings.Join(append([]string{varName}, f.FieldNames()[n+1:]...), ".")
		}
		return fmt.Sprintf(`defs = append(defs, %d)
reps = append(reps, lastRep)
vals = append(vals, %s)`, i, varName)
	}

	fieldName, rt, n, reps := f.NilField(i)
	var nextVar string
	var buf bytes.Buffer
	rc := readClause{
		Var:   varName,
		Field: fieldName,
		Rep:   reps - 1,
		Def:   i,
	}

	if rt == fields.Repeated {
		if reps > 1 {
			rc.Field = f.FieldNames()[n]
		}
		nextVar = fmt.Sprintf("x%d", reps-1)
		readRepeatedRepeatedTpl.Execute(&buf, rc)
	} else {
		nextVar = varName
		if reps > 0 {
			rc.Field = strings.Join(f.FieldNames()[i:], ".")
		}
		readRepeatedOptionalTpl.Execute(&buf, rc)
	}

	return fmt.Sprintf(string(buf.Bytes()), doReadRepeated(f, i+1, nextVar))
}

func lastRepeated(rts []fields.RepetitionType) int {
	var l int
	for i, rt := range rts {
		if rt == fields.Repeated {
			l = i
		}
	}
	return l
}
