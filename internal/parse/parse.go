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
	tagNames  []string
	fieldName string
	fieldType string
	omit      bool
	embedded  bool
	optional  bool
	repeated  bool
	err       error
}

// Result holds the fields and errors that are generated
// by reading a go struct.
type Result struct {
	// Fields are the fields that will be written to and read from a parquet file.
	Parent flds.Field
	// Errors is a list of errors that occurred while parsing a struct.
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

	fields, err := getFields(f.n)
	if err != nil {
		return nil, err
	}

	parent, ok := fields[typ]
	if !ok {
		return nil, fmt.Errorf("could not find %s", typ)
	}

	errs := getChildren(&parent, fields)

	return &Result{
		Parent: flds.Field{Children: parent.Children},
		Errors: errs,
	}, nil
}

func getChildren(parent *flds.Field, fields map[string]flds.Field) []error {
	var children []flds.Field
	var errs []error
	p, ok := fields[parent.FieldType]
	if !ok {
		errs = append(errs, fmt.Errorf("could not find %+v", parent))
	}

	for _, child := range p.Children {
		if child.Primitive() {
			children = append(children, child)
			continue
		}

		f, ok := fields[child.FieldType]
		if !ok {
			errs = append(errs, fmt.Errorf("unsupported type %+v", child.FieldType))
			continue
		}

		errs = append(errs, getChildren(&child, fields)...)

		f.FieldName = child.FieldName
		f.TypeName = child.TypeName
		f.ColumnName = child.ColumnName
		f.Children = child.Children
		f.RepetitionType = child.RepetitionType

		if child.Embedded {
			for _, ch := range f.Children {
				children = append(children, ch)
			}
		} else {
			children = append(children, f)
		}
	}
	parent.Children = children
	return errs
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

func getFields(n map[string]ast.Node) (map[string]fields.Field, error) {
	fields := map[string]flds.Field{}
	for k, n := range n {
		x, ok := n.(*ast.TypeSpec)
		if !ok {
			continue
		}

		parent := flds.Field{
			Type:       x.Name.Name,
			TypeName:   x.Name.Name,
			ColumnName: x.Name.Name,
			FieldName:  x.Name.Name,
			FieldType:  x.Name.Name,
		}

		ast.Inspect(n, func(n ast.Node) bool {
			if n == nil {
				return false
			}

			switch x := n.(type) {
			case *ast.Field:
				if len(x.Names) == 1 && !isPrivate(x) {
					f, skip := getField(x.Names[0].Name, x, nil)
					if !skip {
						parent.Children = append(parent.Children, f)
					}
				} else if len(x.Names) == 0 && !isPrivate(x) {
					f, skip := getField(fmt.Sprintf("%s", x.Type), x, nil)
					f.Embedded = true
					if !skip {
						parent.Children = append(parent.Children, f)
					}
				}
			}
			return true
		})

		fields[k] = parent
	}

	return fields, nil
}

func getType(typ string) string {
	parts := strings.Split(typ, ".")
	return parts[len(parts)-1]
}

func getField(name string, x ast.Node, parent *flds.Field) (flds.Field, bool) {
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

	if tag == "" {
		tag = name
	}

	_, cat, pt, _ := lookupTypeAndCategory(typ, optional, repeated)

	rt := fields.Required
	if repeated {
		rt = fields.Repeated
	} else if optional {
		rt = fields.Optional
	}

	return flds.Field{
		FieldName:  name,
		FieldType:  typ,
		ColumnName: tag,
		TypeName:   getTypeName(typ, optional),
		//Type:           fn,
		ParquetType:    pt,
		Category:       cat,
		RepetitionType: rt,
	}, tag == "-"
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

func lookupTypeAndCategory(name string, optional, repeated bool) (string, string, string, bool) {
	var op string
	if optional || repeated {
		op = "Optional"
	}
	f, ok := types[name]
	if !ok {
		return "", "", "", false
	}
	return fmt.Sprintf(f.name, op, "Field"), fmt.Sprintf(f.category, op), fmt.Sprintf(f.name, "", "Type"), true
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
		//fmt.Printf("node: %+v\n", n)
		f.n[n.Name.Name] = n
		return visitorFunc(f.findTypes)
	}

	return nil
}
