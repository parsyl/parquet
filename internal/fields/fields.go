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
	NthChild       int
	Defined        bool
}

type input struct {
	Parent string
	Val    string
	Append bool
}

func (f Field) StructType() string {
	if f.Parent == nil {
		return f.Type
	}

	var typ string
	for fld := f.Parent; fld != nil; fld = fld.Parent {
		typ = fld.Type
	}
	return typ
}

func (f Field) Fields() []Field {
	return f.fields(0)
}

func (f Field) fields(i int) []Field {
	var out []Field
	for j, fld := range f.Children {
		fld.NthChild = j
		fld.Parent = &f
		if fld.Primitive() {
			out = append(out, fld)
		} else {
			out = append(out, fld.fields(i+1)...)
		}
	}
	return out
}

func (f Field) Chain() []Field {
	out := []Field{f}
	for fld := f.Parent; fld != nil; fld = fld.Parent {
		out = append(out, *fld)
	}
	var defined bool
	for i, fld := range out {
		fld.Defined = defined
		out[i] = fld
		if fld.Parent != nil && fld.NthChild > 0 {
			fld.Parent.Defined = true
			defined = true
		}
	}

	return out
}

func Reverse(out []Field) []Field {
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func (f Field) FieldNames() []string {
	var out []string
	for _, fld := range Reverse(f.Chain()) {
		if fld.FieldName != "" {
			out = append(out, fld.FieldName)
		}
	}
	return out
}

func (f Field) FieldTypes() []string {
	var out []string
	for _, fld := range Reverse(f.Chain()) {
		if fld.FieldType != "" {
			out = append(out, fld.FieldType)
		}
	}
	return out
}

func (f Field) ColumnNames() []string {
	var out []string
	for _, fld := range Reverse(f.Chain()) {
		if fld.ColumnName != "" {
			out = append(out, fld.ColumnName)
		}
	}
	return out
}

func (f Field) RepetitionTypes() RepetitionTypes {
	var out []RepetitionType
	for _, fld := range Reverse(f.Chain()) {
		out = append(out, fld.RepetitionType)
	}
	return out[1:]
}

// DefIndex calculates the index of the
// nested field with the given definition level.
func (f Field) DefIndex(def int) int {
	var count, i int
	for _, fld := range Reverse(f.Chain()) {
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
	for _, fld := range Reverse(f.Chain()) {
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
	for _, fld := range Reverse(f.Chain()) {
		if fld.RepetitionType == Repeated {
			out++
		}
	}
	return out
}

// MaxRepForDef cacluates the largest possible repetition
// level for the nested field at the given definition level.
func (f Field) MaxRepForDef(def int) int {
	var out int
	var defs int
	for _, fld := range Reverse(f.Chain()) {
		if fld.RepetitionType == Repeated || fld.RepetitionType == Optional {
			defs++
		}

		if defs == def {
			return out
		}

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
func (f Field) RepCases() []RepCase {
	mr := int(f.MaxRep())

	if f.RepetitionType != Repeated && f.Parent != nil && f.Parent.RepetitionType == Repeated && f.Parent.Defined {
		return []RepCase{{Case: fmt.Sprintf("case 0, %d:", mr)}}
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
	return Reverse(f.Chain())[i]
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

func (f Field) rightComplete(fld Field, i, def, rep, maxDef, maxRep, defs, reps int) bool {
	if fld.RepetitionType == Optional && rep == 0 && !fld.Defined {
		return true
	}

	if fld.RepetitionType == Repeated && rep > 0 && reps == rep && f.NthChild == 0 {
		return true
	}

	if defs == maxDef && fld.RepetitionType != Required && f.NthChild == 0 {
		return true
	}

	//if rep == 0 && fld.RepetitionType != Required && (fld.RepetitionType == Repeated || f.RepetitionType == Repeated) {
	if rep == 0 && fld.RepetitionType == Repeated && !fld.Defined {
		return true
	}

	return false
}

// Init is called by parquetgen's templates to generate the code
// that writes to a struct's field
//
// example:   x.Friend.Hobby = &Item{}
func (f Field) Init(def, rep int) string {
	maxDef := f.MaxDef()
	maxRep := f.MaxRep()
	var defs, reps int
	var fld Field

	left, right := "%s", "%s"

	chain := f.Chain()

	chain = Reverse(chain)

	var i int
	for _, fld = range chain {
		if fld.Parent == nil {
			continue
		}

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
			if (rep > 0 && reps < rep) || (f.NthChild > 0 && !fld.Primitive()) {
				left = fmt.Sprintf(left, fmt.Sprintf(".%s[ind[%d]]%%s", fld.FieldName, reps-1))
			} else {
				left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.FieldName))
			}
		}

		if f.rightComplete(fld, i, def, rep, maxDef, maxRep, defs, reps) {
			i++
			break
		}

		i++
	}

	left = fmt.Sprintf(left, "")

	for j, fld := range chain[i:] {
		if j > 0 && (fld.RepetitionType == Optional || fld.RepetitionType == Repeated) {
			defs++
		}

		if j > 0 && fld.RepetitionType == Repeated {
			reps++
		}

		switch fld.RepetitionType {
		case Required:
			if fld.Primitive() {
				if (fld.Parent.Parent == nil || fld.Parent.Defined) && fld.Parent.RepetitionType == Repeated && rep == 0 { //Should this be a check for repated anywhere in the full chain?
					right = fmt.Sprintf(right, "vals[nVals]%s")
				} else if (fld.Parent.Parent == nil || fld.Parent.Defined) && rep == 0 {
					right = fmt.Sprintf(right, "vals[0]%s")
				} else if fld.Parent.RepetitionType == Repeated && rep < maxRep { //need one more case:
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: vals[nVals]}%%s", fld.FieldName))
				} else if fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: vals[nVals]%%s", fld.FieldName))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: vals[0]%%s", fld.FieldName))
				}
			} else {
				if fld.Parent.RepetitionType == Repeated && rep < maxRep {
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: %s{%%s}}", fld.FieldName, fld.FieldType))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: %s{%%s}", fld.FieldName, fld.FieldType))
				}
			}
		case Optional:
			if fld.Primitive() {
				if f.NthChild == 0 && fld.Parent.Optional() && !fld.Parent.Repeated() {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: p%s(vals[0])%%s", fld.FieldName, fld.FieldType))
				} else if fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[nVals])%%s", fld.FieldType))
				} else if fld.Parent.Repeated() && f.NthChild == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: p%s(vals[nVals])%%s", fld.FieldName, fld.FieldType))
				} else if fld.Parent.Repeated() && f.NthChild > 0 {
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
				if rep == 0 && fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("{%s: []%s{vals[nVals]}}%%s", fld.FieldName, fld.FieldType))
				} else if (fld.Parent.Parent == nil || fld.Parent.Defined) && rep == 0 {
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

		if def != maxDef && defs >= def {
			break
		}
	}

	right = fmt.Sprintf(right, "")
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
