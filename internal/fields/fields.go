package fields

import (
	"bytes"
	"fmt"
	"log"
	"strings"
)

// Field holds metadata that is required by parquetgen in order
// to generate code.
type Field struct {
	Type            string
	RepetitionTypes RepetitionTypes
	FieldNames      []string
	ColumnNames     []string
	FieldTypes      []string
	Seen            RepetitionTypes
	TypeName        string
	FieldType       string
	ParquetType     string
	Category        string
}

type input struct {
	Parent string
	Val    string
	Append bool
}

// Seen counts how many sub-fields have been previously processed
// so that some of the cases and if statements can be skipped when
// re-assembling records
func Seen(i int, flds []Field) []RepetitionType {
	f := flds[i]
	out := rts([]RepetitionType{})

	l := len(f.FieldNames)
	for _, fld := range flds[:i] {
		end := l
		if len(fld.FieldNames) <= l {
			end = len(fld.FieldNames)
		}
		for i, n := range fld.FieldNames[:end] {
			if n == f.FieldNames[i] {
				out = out.add(i, fld.RepetitionTypes)
			}
		}
	}

	return []RepetitionType(out)
}

// DefIndex calculates the index of the
// nested field with the given definition level.
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

// MaxDef cacluates the largest possible definition
// level for the nested field.
func (f Field) MaxDef() int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == Optional || o == Repeated {
			out++
		}
	}
	return out
}

// MaxRep cacluates the largest possible repetition
// level for the nested field.
func (f Field) MaxRep() int {
	var out int
	for _, o := range f.RepetitionTypes {
		if o == Repeated {
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

	for j, o = range f.RepetitionTypes {
		fields = append(fields, f.FieldNames[j])
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
	return Field{
		RepetitionTypes: f.RepetitionTypes[i:],
		FieldNames:      f.FieldNames[i:],
		FieldTypes:      f.FieldTypes[i:],
	}
}

// Parent returns a parent field based on i
func (f Field) Parent(i int) Field {
	return Field{
		RepetitionTypes: f.RepetitionTypes[:i],
		FieldNames:      f.FieldNames[:i],
		FieldTypes:      f.FieldTypes[:i],
	}
}

// Copy returns a deep copy of the field
func (f Field) Copy() Field {
	return Field{
		RepetitionTypes: append(f.RepetitionTypes[:0:0], f.RepetitionTypes...),
		FieldNames:      append(f.FieldNames[:0:0], f.FieldNames...),
		FieldTypes:      append(f.FieldTypes[:0:0], f.FieldTypes...),
		Seen:            append(f.Seen[:0:0], f.Seen...),
	}
}

// Repeated wraps RepetitionTypes.Repeated()
func (f Field) Repeated() bool {
	return f.RepetitionTypes.Repeated()
}

// Optional wraps RepetitionTypes.Optional()
func (f Field) Optional() bool {
	return f.RepetitionTypes.Optional()
}

// Required wraps RepetitionTypes.Required()
func (f Field) Required() bool {
	return f.RepetitionTypes.Required()
}

// Init is called by parquetgen's templates to generate the code
// that writes to a struct's field (for example: x.Friend.Hobby = &Item{})
func (f Field) Init(def, rep int) string {
	md := f.MaxDef()
	if rep > 0 {
		var count int
		s := Seen(1, []Field{f, f})
		for i, rt := range f.RepetitionTypes {
			if rt == Repeated {
				count++
			}
			if count == rep {
				f.Seen = s[:i]
			}
		}
	}

	start, end := f.start(def, rep), f.end(def, rep)
	flds := make([]field, len(f.RepetitionTypes[start:end]))

	i := start
	var j int
	var nReps int
	for _, rt := range f.RepetitionTypes[start:end] {
		if rt == Repeated {
			nReps++
		}
		flds[j] = field{
			RT:    rt,
			Name:  f.FieldNames[i],
			Type:  f.FieldTypes[i],
			i:     i,
			start: start,
			seen:  f.Seen,
			rep:   rep,
			nReps: nReps,
		}

		i++
		j++
	}

	// start with the innermost field
	flds = reverse(flds)

	var remainder []field
	if len(flds) > 1 {
		remainder = flds[1:]
	}

	if def == md {
		if flds[0].Primitive() && f.RepetitionTypes.Repeated() {
			flds[0].Val = "vals[nVals]"
		} else if flds[0].Primitive() && !f.RepetitionTypes.Repeated() {
			flds[0].Val = "vals[0]"
		}
	}

	in := input{
		Parent: f.parent(start),
		Val:    flds[0].init(remainder),
		Append: f.append(rep, flds[0]),
	}

	var buf bytes.Buffer
	fieldTpl.Execute(&buf, in)
	return buf.String()
}

func (f Field) append(rep int, fld field) bool {
	return rep > 0 ||
		(f.Seen.Repeated() && fld.RT == Repeated)
}

func (f Field) parent(start int) string {
	names := make([]string, start+1)
	var r int
	l := len(f.FieldNames[:start+1])
	for i, n := range f.FieldNames[:start+1] {
		if i < l-1 && f.RepetitionTypes[i] == Repeated {
			n = fmt.Sprintf("%s[ind[%d]]", n, r)
			r++
		}
		names[i] = n
	}
	return strings.Join(names, ".")
}

// Path creates gocode for initializing a string slice in a go template
func (f Field) Path() string {
	out := make([]string, len(f.ColumnNames))
	for i, n := range f.ColumnNames {
		out[i] = fmt.Sprintf(`"%s"`, n)
	}
	return strings.Join(out, ", ")
}

// start calculates which nested field is
// being written to based on the definition
// level and which parent fields have already
// been written to by previous fields with
// common ancestors.
func (f Field) start(def, rep int) int {
	di := f.DefIndex(def)
	seen := f.Seen
	if len(seen) > di {
		seen = seen[:di+1]
	}

	if len(f.RepetitionTypes)-1 > di {
		for _, rt := range f.RepetitionTypes[di+1:] {
			if rt >= Optional {
				break
			}
			di++
		}
	}

	if rep == 0 {
		rep = int(seen.MaxRep()) + 1
	}

	var i, reps int
	var rt RepetitionType
	for i, rt = range f.RepetitionTypes[:di+1] {
		if rt == Required {
			continue
		}

		if rt == Repeated {
			reps++
		}

		if reps == rep {
			break
		}

		if rt >= Optional && i >= len(seen) {
			break
		}
	}

	return i
}

func (f Field) end(def, rep int) int {
	if def == f.MaxDef() {
		return len(f.RepetitionTypes)
	}

	s := f.start(def, rep)

	var i int
	md := int(f.RepetitionTypes[:s].MaxDef())
	for _, rt := range f.RepetitionTypes[s:] {
		if (rt == Optional || rt == Repeated) && i < def-md {
			i++
		}
	}
	return s + i
}

type field struct {
	RT    RepetitionType
	Name  string
	Type  string
	Val   string
	i     int
	start int
	seen  RepetitionTypes
	rep   int
	nReps int
}

func (f field) init(flds []field) string {
	var buf bytes.Buffer
	err := initTpl.Execute(&buf, f)
	if err != nil {
		log.Fatal(err)
	}

	if len(flds) == 0 {
		return buf.String()
	}

	f2 := flds[0]
	var flds2 []field
	if len(flds) > 1 {
		flds2 = flds[1:]
	}

	f2.Val = fmt.Sprintf("%s: %s", f.Name, buf.String())
	return f2.init(flds2)
}

// Slice is called by parquetgen's go templates to determine
// if the field is repeated or not.
func (f field) Slice() bool {
	return (f.RT == Repeated && f.i != f.start) ||
		(f.RT == Repeated && f.rep == 0 && f.i == f.start && !f.seen.NRepeated(f.i+1) && !f.Primitive()) ||
		(f.RT == Repeated && f.rep == 0 && f.Primitive() && f.i == 0)
}

// Primitive is called in order to determine if the field is primitive or not.
func (f field) Primitive() bool {
	return primitiveTypes[f.Type]
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
