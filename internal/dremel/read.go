package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/parse"
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
		return nil, %d
`, nilField(def, f), def)
	}

	var ptr string
	if !f.Optionals[len(f.Optionals)-1] {
		ptr = "&"
	}
	out += fmt.Sprintf(`	default:
		return %sx.%s, %d`, ptr, nilField(n, f), n)

	if !f.Optionals[len(f.Optionals)-1] {
		ptr = "*"
	}

	return fmt.Sprintf(`func read%s(x %s) (%s%s, int64) {
	switch {
	%s
	}
}`, strings.Join(f.FieldNames, ""), f.Type, ptr, f.TypeName, out)
}
