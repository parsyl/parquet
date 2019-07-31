package fields

import (
	"fmt"
	"strings"
)

type RepetitionType int

const (
	Required RepetitionType = 0
	Optional RepetitionType = 1
	Repeated RepetitionType = 2
)

type RepetitionTypes []RepetitionType

func (r RepetitionTypes) MaxDef() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Optional || rt == Repeated {
			out++
		}
	}
	return out
}

func (r RepetitionTypes) MaxRep() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Repeated {
			out++
		}
	}
	return out
}

type Field struct {
	Type            string
	FieldNames      []string
	FieldTypes      []string
	TypeName        string
	FieldType       string
	ParquetType     string
	ColumnName      string
	Category        string
	RepetitionTypes []RepetitionType
}

func (f Field) Child(i int) Field {
	return Field{
		FieldNames:      f.FieldNames[i:],
		FieldTypes:      f.FieldTypes[i:],
		RepetitionTypes: f.RepetitionTypes[i:],
	}
}

func (f Field) DefChild(def int) Field {
	i := f.DefIndex(def)
	if i >= len(f.FieldNames) {
		return Field{
			FieldNames:      nil,
			FieldTypes:      nil,
			RepetitionTypes: nil,
		}
	}
	return Field{
		FieldNames:      f.FieldNames[i:],
		FieldTypes:      f.FieldTypes[i:],
		RepetitionTypes: f.RepetitionTypes[i:],
	}
}

func (f Field) Optional() bool {
	for _, t := range f.RepetitionTypes {
		if t == Optional {
			return true
		}
	}
	return false
}

func (f Field) Required() bool {
	for _, t := range f.RepetitionTypes {
		if t != Required {
			return false
		}
	}
	return true
}

func (f Field) Repeated() bool {
	for _, t := range f.RepetitionTypes {
		if t == Repeated {
			return true
		}
	}
	return false
}

func (f Field) MaxDef() int {
	var out int
	for _, t := range f.RepetitionTypes {
		if t == Optional || t == Repeated {
			out++
		}
	}
	return out
}

func (f Field) DefIndex(def int) int {
	var count int
	for j, o := range f.RepetitionTypes {
		if o == Optional || o == Repeated {
			count++
		}
		if count == def {
			return j
		}
	}
	return def
}

func (f Field) NthDef(i int) (int, RepetitionType) {
	var count int
	var out RepetitionType
	var x int
	for _, t := range f.RepetitionTypes {
		if t == Optional || t == Repeated {
			count++
			if count == i {
				out = t
				x = count
			}
		}
	}
	return x, out
}

func (f Field) Defs() []int {
	out := make([]int, 0, len(f.RepetitionTypes))
	for i, t := range f.RepetitionTypes {
		if t == Optional {
			out = append(out, i+1)
		}
	}
	return out
}

func (f Field) MaxRep() uint {
	var out uint
	for _, t := range f.RepetitionTypes {
		if t == Repeated {
			out++
		}
	}
	return out
}

type RepCase struct {
	Case string
	Rep  int
}

func (f Field) RepCases(seen int) []RepCase {
	mr := int(f.MaxRep())
	if mr == seen {
		return []RepCase{{Case: "default:"}}
	}

	var out []RepCase
	for i := 0; i <= mr; i++ {
		out = append(out, RepCase{Case: fmt.Sprintf("case %d:", i), Rep: i})
	}
	return out
}

func (f Field) NilField(i int) (string, RepetitionType, int, int) {
	var fields []string
	var count int
	var j, reps int
	var o RepetitionType

	for j, o = range f.RepetitionTypes {
		fields = append(fields, f.FieldNames[j])
		if o == Optional {
			count++
		} else if o == Repeated {
			count++
			reps++
		}
		if count > i {
			break
		}
	}
	return strings.Join(fields, "."), o, j, reps
}

func (f Field) RepetitionType() string {
	if f.RepetitionTypes[len(f.RepetitionTypes)-1] == Optional {
		return "parquet.RepetitionOptional"
	}
	return "parquet.RepetitionRequired"
}

func (f Field) Path() string {
	out := make([]string, len(f.FieldNames))
	for i, n := range f.FieldNames {
		out[i] = fmt.Sprintf(`"%s"`, strings.ToLower(n))
	}
	return strings.Join(out, ", ")
}
