package fields

import (
	"fmt"
	"strings"
)

// Field holds metadata that is required by parquetgen in order
// to generate code.
type Field struct {
	// Type of the top level struct
	Type           string
	RepetitionType RepetitionType
	FieldName      string
	ColumnName     string
	TypeName       string
	FieldType      string
	ParquetType    string
	Category       string
	Parent         *Field
	Embedded       bool
	Children       []Field
}

type input struct {
	Parent string
	Val    string
	Append bool
}

func (f Field) chain() []Field {
	out := []Field{f}
	for fld := f.Parent; fld != nil; fld = fld.Parent {
		out = append(out, *fld)
	}
	return out
}

func reverse(out []Field) []Field {
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func (f Field) FieldNames() []string {
	var out []string
	for _, fld := range reverse(f.chain()) {
		out = append(out, fld.FieldName)
	}
	return out
}

func (f Field) FieldTypes() []string {
	var out []string
	for _, fld := range reverse(f.chain()) {
		out = append(out, fld.FieldType)
	}
	return out
}

func (f Field) ColumnNames() []string {
	var out []string
	for _, fld := range reverse(f.chain()) {
		out = append(out, fld.ColumnName)
	}
	return out
}

func (f Field) RepetitionTypes() RepetitionTypes {
	var out []RepetitionType
	for _, fld := range reverse(f.chain()) {
		out = append(out, fld.RepetitionType)
	}
	return out
}

// DefIndex calculates the index of the
// nested field with the given definition level.
func (f Field) DefIndex(def int) int {
	var count, i int
	for _, fld := range reverse(f.chain()) {
		if fld.RepetitionType == Optional || fld.RepetitionType == Repeated {
			count++
		}
		if count == def {
			return i
		}
		i++
	}
	return def
}

// MaxDef cacluates the largest possible definition
// level for the nested field.
func (f Field) MaxDef() int {
	var out int
	for _, fld := range reverse(f.chain()) {
		if fld.RepetitionType == Optional || fld.RepetitionType == Repeated {
			out++
		}
	}
	return out
}

// MaxRep cacluates the largest possible repetition
// level for the nested field.
func (f Field) MaxRep() int {
	var out int
	for _, fld := range reverse(f.chain()) {
		if fld.RepetitionType == Repeated {
			out++
		}
	}
	return out
}

// RepCase is used by parquetgen to generate code.
type RepCase struct {
	// Case is the code for a switch case (for example: case 0:)
	Case string
	// Rep is the repetition level that is handled by the switch case.
	Rep int
}

// RepCases returns a RepCase slice based on the field types and
// what sub-fields have already been seen.
func (f Field) RepCases(seen RepetitionTypes) []RepCase {
	mr := int(f.MaxRep())
	if mr == int(seen.MaxRep()) {
		return []RepCase{{Case: "default:"}}
	}

	var out []RepCase
	for i := 0; i <= mr; i++ {
		out = append(out, RepCase{Case: fmt.Sprintf("case %d:", i), Rep: i})
	}
	return out
}

// NilField finds the nth field that is optional and returns some
// information about it.
func (f Field) NilField(n int) (string, RepetitionType, int, int) {
	var fields []string
	var count int
	var j, reps int
	var o RepetitionType

	fieldNames := f.FieldNames()
	for j, o = range f.RepetitionTypes() {
		fields = append(fields, fieldNames[j])
		if o == Optional {
			count++
		} else if o == Repeated {
			count++
			reps++
		}
		if count > n {
			break
		}
	}
	return strings.Join(fields, "."), o, j, reps
}

// Child returns a sub-field based on i
func (f Field) Child(i int) Field {
	return reverse(f.chain())[i]
}

// Repeated wraps RepetitionTypes.Repeated()
func (f Field) Repeated() bool {
	return f.RepetitionTypes().Repeated()
}

// Optional wraps RepetitionTypes.Optional()
func (f Field) Optional() bool {
	return f.RepetitionTypes().Optional()
}

// Required wraps RepetitionTypes.Required()
func (f Field) Required() bool {
	return f.RepetitionTypes().Required()
}

// Init is called by parquetgen's templates to generate the code
// that writes to a struct's field
//
// example:   x.Friend.Hobby = &Item{}
func (f Field) Init(def, rep, nthChild int) string {
	maxDef := f.MaxDef()
	maxRep := f.MaxRep()
	var defs, reps int
	var fld Field

	left, right := "%s", "%s"

	chain := reverse(f.chain())

	var i int
	for i, fld = range chain {
		if fld.RepetitionType == Optional || fld.RepetitionType == Repeated {
			defs++
		}

		if fld.RepetitionType == Repeated {
			reps++
		}

		switch fld.RepetitionType {
		case Required:
			left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.FieldName))
		case Optional:
			left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.FieldName))
		case Repeated:
			if (rep > 0 && reps < rep) || (nthChild > 0 && !fld.Primitive()) {
				left = fmt.Sprintf(left, fmt.Sprintf(".%s[ind[%d]]%%s", fld.FieldName, reps-1))
			} else {
				left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.FieldName))
			}
		}

		if (defs >= def || ((rep == 0 && fld.RepetitionType != Required) || (rep > 0 && reps == rep))) && nthChild == 0 {
			break
		}
	}

	left = fmt.Sprintf(left, "")
	defs = 0
	for j, fld := range chain[i:] {
		if fld.RepetitionType == Optional || fld.RepetitionType == Repeated {
			defs++
		}

		if j > 0 && fld.RepetitionType == Repeated {
			reps++
		}

		switch fld.RepetitionType {
		case Required:
			if fld.Primitive() {
				if fld.Parent.RepetitionType == Repeated && rep < maxRep { //need one more case:
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: vals[nVals]}%%s", fld.FieldName))
				} else if fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: vals[nVals]%%s", fld.FieldName))
				} else if nthChild > 0 {
					right = fmt.Sprintf(right, "vals[0]%s")
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: vals[0]%%s", fld.FieldName))
				}
			} else {
				if fld.Parent.RepetitionType == Repeated && rep < maxRep {
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: %s{%%s}}", fld.FieldType, fld.FieldName))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: %s{%%s}", fld.FieldType, fld.FieldName))
				}
			}
		case Optional:
			if fld.Primitive() {
				if nthChild == 0 && fld.Parent.Optional() && !fld.Parent.Repeated() {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: p%s(vals[0])%%s", fld.FieldName, fld.FieldType))
				} else if fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[nVals])%%s", fld.FieldType))
				} else if fld.Parent.Repeated() && nthChild == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: p%s(vals[nVals])%%s", fld.FieldName, fld.FieldType))
				} else if fld.Parent.Repeated() && nthChild > 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[nVals])%%s", fld.FieldType))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[0])%%s", fld.FieldType))
				}
			} else {
				if j == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("&%s{%%s}", fld.FieldType))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: &%s{%%s}", fld.FieldName, fld.FieldType))
				}
			}
		case Repeated:
			if fld.Primitive() {
				if rep == 0 && fld.Parent != nil && fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: []%s{vals[nVals]}}%%s", fld.FieldName, fld.FieldType))
				} else if fld.Parent == nil && rep == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("[]%s{vals[nVals]}%%s", fld.FieldType))
				} else if rep == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: []%s{vals[nVals]}%%s", fld.FieldName, fld.FieldType))
				} else if reps == rep {
					right = fmt.Sprintf(right, fmt.Sprintf("append(x%s, vals[nVals])%%s", left))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("[%s: []%s{vals[nVals]}]%%s", fld.FieldName, fld.FieldType))
				}
			} else {
				if rep == 0 && j == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("[]%s{%%s}", fld.FieldType))
				} else if rep == 0 && reps == maxRep && fld.Parent != nil && fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: []%s{%%s}}", fld.FieldName, fld.FieldType))
				} else if rep == 0 && reps == maxRep {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: []%s{%%s}", fld.FieldName, fld.FieldType))
				} else if reps == rep {
					right = fmt.Sprintf(right, fmt.Sprintf("append(x%s, %s{%%s})", left, fld.FieldType))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: []%s{%%s}", fld.FieldName, fld.FieldType))
				}
			}
		}

		if defs >= def && fld.RepetitionType != Required && def < maxDef {
			break
		}
	}

	right = fmt.Sprintf(right, "")
	fmt.Printf("x%s = %s\n", left, right)
	return fmt.Sprintf("x%s = %s", left, right)
}

// Path creates gocode for initializing a string slice in a go template
func (f Field) Path() string {
	names := f.ColumnNames()
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf(`"%s"`, n)
	}
	return strings.Join(out, ", ")
}

// Primitive is called in order to determine if the field is primitive or not.
func (f Field) Primitive() bool {
	return primitiveTypes[f.FieldType]
}

var primitiveTypes = map[string]bool{
	"bool":    true,
	"int32":   true,
	"uint32":  true,
	"int64":   true,
	"uint64":  true,
	"float32": true,
	"float64": true,
	"string":  true,
}
