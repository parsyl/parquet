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
