package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/fields"
)

func readRequired(f fields.Field) string {
	return fmt.Sprintf(`func read%s(x %s) %s {
	return x.%s
}`, strings.Join(f.FieldNames, ""), f.Type, f.TypeName, strings.Join(f.FieldNames, "."))
}

func readOptional(f fields.Field) string {
	var out string
	n := defs(f)
	for def := 0; def < n; def++ {
		out += fmt.Sprintf(`case x.%s == nil:
		return nil, []uint8{%d}, nil
`, nilField(def, f), def)
	}

	var ptr string
	if f.RepetitionTypes[len(f.RepetitionTypes)-1] == fields.Optional {
		ptr = "*"
	}
	out += fmt.Sprintf(`	default:
		return []%s{%sx.%s}, []uint8{%d}, nil`, cleanTypeName(f.TypeName), ptr, nilField(n, f), n)

	return fmt.Sprintf(`func read%s(x %s) ([]%s, []uint8, []uint8) {
	switch {
	%s
	}
}`, strings.Join(f.FieldNames, ""), f.Type, cleanTypeName(f.TypeName), out)
}

func cleanTypeName(s string) string {
	return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
}
