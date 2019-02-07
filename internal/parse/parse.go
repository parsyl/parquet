package parse

import (
	"fmt"
	"go/parser"
	"go/token"
	"log"
	"strings"

	"go/ast"
)

const letters = "abcdefghijklmnopqrstuvwxyz"

type Field struct {
	Type        string
	FieldName   string
	TypeName    string
	FieldType   string
	ParquetType string
	ColumnName  string
	Category    string
}

type field struct {
	Field    Field
	tagName  string
	omit     bool
	embedded bool
	err      error
}

type Result struct {
	Fields []Field
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
		if f.err != nil {
			errs = append(errs, f.err)
		} else if f.err == nil && f.embedded {
			embeddedFields := fields[f.Field.TypeName]
			out = append(out[:i], append(embeddedFields, out[i:]...)...)
			i += len(embeddedFields)
		} else if f.err == nil {
			out = append(out, f)
			i++
		}
	}

	return &Result{
		Fields: getFields(fullTyp, out),
		Errors: errs,
	}, nil
}

func getType(typ string) string {
	parts := strings.Split(typ, ".")
	return parts[len(parts)-1]
}

func getFields(typ string, fields []field) []Field {
	out := make([]Field, 0, len(fields))
	for _, f := range fields {
		if !f.omit {
			f.Field.Type = typ
			if f.tagName != "" {
				f.Field.ColumnName = f.tagName
			} else {
				f.Field.ColumnName = f.Field.FieldName
			}
			out = append(out, f.Field)
		}
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
					fields[k] = append(fields[k], field{embedded: true, Field: Field{TypeName: fmt.Sprintf("%s", x.Type)}})
				}
			}
			return true
		})
	}
	return fields, nil
}

func getField(name string, x ast.Node) field {
	var typ, tag string
	var optional bool
	ast.Inspect(x, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.Field:
			if t.Tag != nil {
				tag = parseTag(t.Tag.Value)
			}
		case *ast.StarExpr:
			optional = true
		case ast.Expr:
			s := fmt.Sprintf("%v", t)
			_, ok := types[s]
			if ok {
				typ = s
			}
		}
		return true
	})

	var err error
	_, ok := types[typ]
	if !ok {
		err = fmt.Errorf("unsupported type: %s", name)
	}

	fn, cat, pt := lookupTypeAndCategory(typ, optional)
	return field{Field: Field{FieldName: name, TypeName: getTypeName(typ, optional), FieldType: fn, ParquetType: pt, Category: cat}, tagName: tag, omit: tag == "-", err: err}
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

func lookupTypeAndCategory(name string, optional bool) (string, string, string) {
	var op string
	if optional {
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
