package dremel

import (
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/parse"
)

func init() {
	var err error
	readRepeatedTpl, err = template.New("output").Parse(readRepeatedText)
	if err != nil {
		log.Fatal(err)
	}
}

type readClause struct {
	Clause    string
	Else      string
	LastRep   string
	AppendVal string
}

var (
	readRepeatedTpl  *template.Template
	readRepeatedText = `{{.Clause}} {
		{{.LastRep}}defs = append(defs, {{.Def}})
		reps = append(reps, lastRep){{.AppendVal}}
	}{{.Else}}`
)

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

func doReadRepeated(f parse.Field, i int) string {
	if i > int(f.MaxDef()) {
		return ""
	}

	name, rt := f.NilField(i)
	var ifStm string
	if rt == parse.Optional {
		ifStm = fmt.Sprintf("x.%s == nil", name)
	} else {
		ifStm = fmt.Sprintf("len(x.%s) == 0", name)
	}

	var lastRep string
	if i > 0 && rt == parse.Repeated {
		lastRep = fmt.Sprintf(`if i%d > 0 {
	lastRep = %d
}`, i, i)
	}

	rc := readClause{
		If:      ifStm,
		LastRep: lastRep,
	}

}
