package parse

import (
	"fmt"
	"go/parser"
	"go/token"
	"log"
	"strings"

	"go/ast"

	"github.com/parsyl/parquet/internal/fields"
	flds "github.com/parsyl/parquet/internal/fields"
)

const letters = "abcdefghijklmnopqrstuvwxyz"

type field struct {
	Field     fields.Field
	tagName   string
	fieldName string
	fieldType string
	omit      bool
	embedded  bool
	optional  bool
	repeated  bool
	err       error
}

type Result struct {
	Fields []flds.Field
	Errors []error
}

// Fields gets the fields of the given struct.
// pth must be a go file that defines the typ struct.
// Any embedded structs must also be in that same file.
func Fields(typ, pth string) (*Result, error) {
	fullTyp := typ
	typ = getType(fullTyp)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, pth, nil, 0)
	if err != nil {
		log.Fatal(err)
	}

	f := &finder{n: map[string]ast.Node{}}

	ast.Walk(visitorFunc(f.findTypes), file)

	if f.n == nil {
		return nil, fmt.Errorf("could not find %s", typ)
	}

	fields, err := doGetFields(f.n)
	if err != nil {
		return nil, err
	}

	var out []field
	var errs []error
	var i int

	for _, f := range fields[typ] {
		i, out, errs = getOut(i, f, fields, errs, out)
	}

	return &Result{
		Fields: getFields(fullTyp, out, fields),
		Errors: errs,
	}, nil
}

func getOut(i int, f field, fields map[string][]field, errs []error, out []field) (int, []field, []error) {
	ff, ok := fields[f.fieldType]
	var o flds.RepetitionType = flds.Required
	if strings.Contains(f.Field.TypeName, "*") {
		o = flds.Optional
	} else if f.repeated || strings.Contains(f.Field.TypeName, "[]") {
		o = flds.Repeated
	}
	if ok {
		for _, fld := range ff {
			if fld.embedded {
				x, more, moreerrs := getOut(0, fld, fields, nil, nil)
				i += x
				out = append(out, more...)
				errs = append(errs, moreerrs...)
			} else {
				if (!fld.optional && (o == flds.Optional || f.optional)) || (!fld.repeated && (o == flds.Repeated || f.repeated)) {
					fld = makeOptional(fld)
				}

				if !f.embedded {
					fld.Field.RepetitionTypes = append(append(f.Field.RepetitionTypes[:0:0], f.Field.RepetitionTypes...), o) //make a copy
					fld.Field.FieldNames = append(f.Field.FieldNames, fld.Field.FieldNames...)
					fld.Field.FieldTypes = append(f.Field.FieldTypes, fld.Field.FieldTypes...)
				}
				i, out, errs = getOut(i, fld, fields, errs, out)
			}
		}
		return i, out, errs
	} else if f.err == nil && f.embedded {
		embeddedFields := fields[f.Field.TypeName]
		for j, ef := range embeddedFields {
			var rt flds.RepetitionType = flds.Required
			if strings.Contains(f.Field.TypeName, "*") {
				rt = flds.Optional
			}
			ef.Field.RepetitionTypes = append(ef.Field.RepetitionTypes, rt)
			embeddedFields[j] = f
		}
		out = append(out[:i], append(embeddedFields, out[i:]...)...)
		i += len(embeddedFields)
	} else if f.err == nil {
		_, ok := types[f.fieldType]
		if ok {
			f.Field.RepetitionTypes = append(f.Field.RepetitionTypes, o)
			out = append(out, f)
			i++
		} else {
			errs = append(errs, fmt.Errorf("unsupported type: %s", f.fieldName))
		}
	}
	return i, out, errs
}

func makeOptional(f field) field {
	f.optional = true
	fn, cat, pt := lookupTypeAndCategory(strings.Replace(strings.Replace(f.Field.TypeName, "*", "", 1), "[]", "", 1), true, true)
	f.Field.FieldType = fn
	f.Field.ParquetType = pt
	f.Field.Category = cat
	return f
}

func getType(typ string) string {
	parts := strings.Split(typ, ".")
	return parts[len(parts)-1]
}

func getFields(fullTyp string, fields []field, m map[string][]field) []flds.Field {
	typ := getType(fullTyp)
	out := make([]flds.Field, 0, len(fields))
	for _, f := range fields {
		_, ok := m[typ]
		if f.omit || !ok {
			continue
		}

		if f.repeated {
			f.Field.TypeName = fmt.Sprintf("[]%s", f.Field.TypeName)
		}

		f.Field.Type = fullTyp
		if f.tagName != "" {
			f.Field.ColumnName = f.tagName
		} else {
			f.Field.ColumnName = strings.Join(f.Field.FieldNames, ".")
		}
		out = append(out, f.Field)
	}
	return out
}

func isPrivate(x *ast.Field) bool {
	var s string
	if len(x.Names) == 0 {
		s = fmt.Sprintf("%s", x.Type)
	} else {
		s = fmt.Sprintf("%s", x.Names[0])
	}
	return strings.Contains(letters, string(s[0]))
}

func doGetFields(n map[string]ast.Node) (map[string][]field, error) {
	fields := map[string][]field{}
	for k, n := range n {
		ast.Inspect(n, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.Field:
				if len(x.Names) == 1 && !isPrivate(x) {
					f := getField(x.Names[0].Name, x)
					fields[k] = append(fields[k], f)
				} else if len(x.Names) == 0 && !isPrivate(x) {
					fields[k] = append(fields[k], field{embedded: true, fieldType: fmt.Sprintf("%s", x.Type), Field: flds.Field{TypeName: fmt.Sprintf("%s", x.Type)}})
				}
			case *ast.ArrayType:
				s := fields[k]
				f := s[len(s)-1]
				f.repeated = true
				s[len(s)-1] = f
				fields[k] = s
			}
			return true
		})
	}
	return fields, nil
}

func getField(name string, x ast.Node) field {
	var typ, tag string
	var optional, repeated bool
	ast.Inspect(x, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.Field:
			if t.Tag != nil {
				tag = parseTag(t.Tag.Value)
			}
			typ = fmt.Sprintf("%s", t.Type)
		case *ast.ArrayType:
			at := n.(*ast.ArrayType)
			s := fmt.Sprintf("%v", at.Elt)
			typ = s
			repeated = true
		case *ast.StarExpr:
			optional = true
			typ = fmt.Sprintf("%s", t.X)
		case ast.Expr:
			s := fmt.Sprintf("%v", t)
			_, ok := types[s]
			if ok {
				typ = s
			}
		}
		return true
	})

	fn, cat, pt := lookupTypeAndCategory(typ, optional, repeated)
	return field{
		Field: flds.Field{
			FieldNames:  []string{name},
			FieldTypes:  []string{typ},
			TypeName:    getTypeName(typ, optional),
			FieldType:   fn,
			ParquetType: pt,
			Category:    cat},
		fieldName: name,
		fieldType: typ,
		tagName:   tag,
		omit:      tag == "-",
		optional:  optional,
		repeated:  repeated,
	}
}

func parseTag(t string) string {
	i := strings.Index(t, `parquet:"`)
	if i == -1 {
		return ""
	}
	t = t[i+9:]
	return t[:strings.Index(t, `"`)]
}

func getTypeName(s string, optional bool) string {
	var star string
	if optional {
		star = "*"
	}
	return fmt.Sprintf("%s%s", star, s)
}

func lookupTypeAndCategory(name string, optional, repeated bool) (string, string, string) {
	var op string
	if optional || repeated {
		op = "Optional"
	}
	f, ok := types[name]
	if !ok {
		return "", "", ""
	}
	return fmt.Sprintf(f.name, op, "Field"), fmt.Sprintf(f.category, op), fmt.Sprintf(f.name, "", "Type")
}

type fieldType struct {
	name     string
	category string
}

var types = map[string]fieldType{
	"int32":   {"Int32%s%s", "numeric%s"},
	"uint32":  {"Uint32%s%s", "numeric%s"},
	"int64":   {"Int64%s%s", "numeric%s"},
	"uint64":  {"Uint64%s%s", "numeric%s"},
	"float32": {"Float32%s%s", "numeric%s"},
	"float64": {"Float64%s%s", "numeric%s"},
	"bool":    {"Bool%s%s", "bool%s"},
	"string":  {"String%s%s", "string%s"},
}

type visitorFunc func(n ast.Node) ast.Visitor

func (f visitorFunc) Visit(n ast.Node) ast.Visitor {
	return f(n)
}

type finder struct {
	n map[string]ast.Node
}

func (f *finder) findTypes(n ast.Node) ast.Visitor {
	switch n := n.(type) {
	case *ast.ImportSpec:
		return visitorFunc(f.findTypes)
	case *ast.Package:
		return visitorFunc(f.findTypes)
	case *ast.File:
		return visitorFunc(f.findTypes)
	case *ast.GenDecl:
		if n.Tok == token.TYPE {
			return visitorFunc(f.findTypes)
		}
	case *ast.TypeSpec:
		f.n[n.Name.Name] = n
		return visitorFunc(f.findTypes)
	}

	return nil
}
