package dremel

import (
	"fmt"
	"strings"

	"github.com/parsyl/parquet/internal/fields"
)

func readRequired(f fields.Field) string {
	return fmt.Sprintf(`func read%s(x %s) %s {
	return x.%s
}`, strings.Join(f.FieldNames(), ""), f.StructType(), f.TypeName(), strings.Join(f.FieldNames(), "."))
}

func readOptional(f fields.Field) string {
	var out string
	n := f.MaxDef()
	for def := 0; def < n; def++ {
		out += fmt.Sprintf(`case x.%s == nil:
			return nil, []uint8{%d}, nil
	`, nilField(def, f), def)
	}

	var ptr string
	rts := f.RepetitionTypes()
	if rts[len(rts)-1] == fields.Optional {
		ptr = "*"
	}

	out += fmt.Sprintf(`	default:
			return []%s{%sx.%s}, []uint8{%d}, nil`, cleanTypeName(f.Type), ptr, nilField(n, f), n)

	return fmt.Sprintf(`func read%s(x %s) ([]%s, []uint8, []uint8) {
		switch {
		%s
		}
	}`, strings.Join(f.FieldNames(), ""), f.StructType(), cleanTypeName(f.Type), out)
}

func cleanTypeName(s string) string {
	return strings.Replace(strings.Replace(s, "*", "", 1), "[]", "", 1)
}

func nilField(i int, f fields.Field) string {
	var flds []string
	var count int
	for j, o := range f.RepetitionTypes() {
		flds = append(flds, f.FieldNames()[j])
		if o == fields.Optional {
			count++
		}
		if count > i {
			break
		}
	}
	return strings.Join(flds, ".")
}
