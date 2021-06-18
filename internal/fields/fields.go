package fields

import (
	"fmt"
	"strings"
)

// Field holds metadata that is required by parquetgen in order
// to generate code.
type Field struct {
	Type           string
	Name           string
	ColumnName     string
	RepetitionType RepetitionType
	Parent         *Field
	Children       []Field
	Embedded       bool
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

func (f Field) IsRoot() bool {
	return f.Parent == nil
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
		if fld.Name != "" {
			out = append(out, fld.Name)
		}
	}
	return out
}

func (f Field) FieldTypes() []string {
	var out []string
	for _, fld := range Reverse(f.Chain()) {
		if fld.Type != "" {
			out = append(out, fld.Type)
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
	Reps []int
	// Rep is the repetition level that is handled by the switch case.
	Rep int

	// Repeated is true if any of the fields (including the one at the def level) were repeated
	// This allows the def case to not have a rep case for fields that have a repetition somewhere
	// in the chain.
	Repeated bool
}

func (r RepCase) Case() string {
	return fmt.Sprintf(
		"case %s:",
		strings.Trim(strings.Replace(fmt.Sprint(r.Reps), " ", ", ", -1), "[]"),
	)
}

type RepCases []RepCase

func (r RepCases) UseRepCase(f Field, def int) bool {
	if f.Parent.IsRoot() {
		return false
	}
	return len(r) > 1 ||
		(len(r) == 1 && r[0].Repeated && r[0].Rep < f.MaxRepForDef(def))
}

// RepCases returns a RepCase slice based on the field types and
// what sub-fields have already been seen.
func (f Field) RepCases(def int) RepCases {
	mr := int(f.MaxRep())

	var out []RepCase
	var defs int
	var reps int
	rollup := []int{0}
	i := 1
	for _, fld := range Reverse(f.Chain()) {
		if fld.IsRoot() {
			continue
		}

		if defs == def && fld.RepetitionType != Required {
			break
		}

		if fld.RepetitionType == Optional || fld.RepetitionType == Repeated {
			defs++
		}

		if fld.RepetitionType == Repeated && reps < mr && defs <= def {
			reps++
			rollup = append(rollup, reps)
		}

		if len(rollup) > 0 && (!fld.Defined || (defs == def && fld.RepetitionType != Required)) {
			out = append(out, RepCase{Reps: rollup[:], Rep: max(rollup), Repeated: reps > 0})
			rollup = []int{}
		}

		i++
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

func (f Field) leftComplete(fld Field, i, def, rep, maxDef, maxRep, defs, reps int) bool {
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

func (f Field) rightComplete(def, defs, maxDef int) bool {
	return def != maxDef && defs >= def
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
			left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.Name))
		case Optional:
			left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.Name))
		case Repeated:
			if fld.Primitive() || f.leftComplete(fld, i, def, rep, maxDef, maxRep, defs, reps) {
				left = fmt.Sprintf(left, fmt.Sprintf(".%s%%s", fld.Name))
			} else {
				left = fmt.Sprintf(left, fmt.Sprintf(".%s[ind[%d]]%%s", fld.Name, reps-1))
			}
		}

		if f.leftComplete(fld, i, def, rep, maxDef, maxRep, defs, reps) {
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
				if (fld.Parent.IsRoot() || fld.Parent.Defined) && fld.Parent.RepetitionType == Repeated && (rep == 0 || rep == reps) { //Should this be a check for repeated anywhere in the full chain?
					right = fmt.Sprintf(right, "vals[nVals]%s")
				} else if (fld.Parent.Parent == nil || fld.Parent.Defined) && rep == 0 {
					right = fmt.Sprintf(right, "vals[0]%s")
				} else if fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: vals[nVals]%%s", fld.Name))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: vals[0]%%s", fld.Name))
				}
			} else {
				right = fmt.Sprintf(right, fmt.Sprintf("%s: %s{%%s}", fld.Name, fld.Type))
			}
		case Optional:
			if fld.Primitive() {
				if f.NthChild == 0 && fld.Parent.Optional() && !fld.Parent.Repeated() {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: p%s(vals[0])%%s", fld.Name, fld.Type))
				} else if fld.Parent.RepetitionType == Repeated {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[nVals])%%s", fld.Type))
				} else if fld.Parent.Repeated() && f.NthChild == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: p%s(vals[nVals])%%s", fld.Name, fld.Type))
				} else if fld.Parent.Repeated() && f.NthChild > 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[nVals])%%s", fld.Type))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("p%s(vals[0])%%s", fld.Type))
				}
			} else {
				if j == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("&%s{%%s}", fld.Type))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: &%s{%%s}", fld.Name, fld.Type))
				}
			}
		case Repeated:
			if fld.Primitive() {
				if j == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("append(x%s, vals[nVals])%%s", left))
				} else if !fld.IsRoot() {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: []%s{vals[nVals]}%%s", fld.Name, fld.Type))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("[]%s{vals[nVals]}%%s", fld.Type))
				}
			} else {
				if rep > 0 && reps == rep || (fld.MaxRepForDef(def) == rep && !strings.Contains(right, "append(")) {
					right = fmt.Sprintf(right, fmt.Sprintf("append(x%s, %s{%%s})", left, fld.Type))
				} else if rep == 0 && j == 0 && !f.rightComplete(def, defs, maxDef) {
					right = fmt.Sprintf(right, fmt.Sprintf("[]%s{{%%s}}", fld.Type))
				} else if rep == 0 && j == 0 {
					right = fmt.Sprintf(right, fmt.Sprintf("[]%s{%%s}", fld.Type))
				} else if (!f.rightComplete(def, defs, maxDef) && !chain[j+1].Primitive()) || (f.rightComplete(def, defs, maxDef) && def == defs) {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: []%s{{%%s}}", fld.Name, fld.Type))
				} else {
					right = fmt.Sprintf(right, fmt.Sprintf("%s: []%s{%%s}", fld.Name, fld.Type))
				}
			}
		}

		if f.rightComplete(def, defs, maxDef) {
			break
		}
	}

	right = fmt.Sprintf(right, "")
	return fmt.Sprintf("x%s = %s", left, right)
}

// IsRep is true if this fields is one being repeated
func (f Field) IsRep(rep int) bool {
	var reps int
	for _, fld := range Reverse(f.Chain()) {
		if fld.RepetitionType == Repeated {
			reps++
		}
	}

	return reps == rep
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
	_, ok := primitiveTypes[f.Type]
	return ok
}

func (f Field) FieldType() string {
	var op string
	if f.Optional() || f.Repeated() {
		op = "Optional"
	}

	ft := primitiveTypes[f.Type]
	return fmt.Sprintf(ft.name, op, "Field")
}

func (f Field) ParquetType() string {
	ft := primitiveTypes[f.Type]
	return fmt.Sprintf(ft.name, "", "Type")
}

func (f Field) Category() string {
	var op string
	if f.Optional() || f.Repeated() {
		op = "Optional"
	}

	ft := primitiveTypes[f.Type]
	return fmt.Sprintf(ft.category, op)
}

func (f Field) TypeName() string {
	var star string
	if f.RepetitionType == Optional {
		star = "*"
	}
	return fmt.Sprintf("%s%s", star, f.Type)
}

type fieldType struct {
	name     string
	category string
}

var primitiveTypes = map[string]fieldType{
	"int32":   {"Int32%s%s", "numeric%s"},
	"uint32":  {"Uint32%s%s", "numeric%s"},
	"int64":   {"Int64%s%s", "numeric%s"},
	"uint64":  {"Uint64%s%s", "numeric%s"},
	"float32": {"Float32%s%s", "numeric%s"},
	"float64": {"Float64%s%s", "numeric%s"},
	"bool":    {"Bool%s%s", "bool%s"},
	"string":  {"String%s%s", "string%s"},
}

func max(i []int) int {
	return i[len(i)-1]
}
