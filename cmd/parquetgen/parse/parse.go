package parse

import (
	"fmt"
	"go/parser"
	"go/token"
	"log"
	"strings"

	"go/ast"

	"github.com/parsyl/parquet/cmd/parquetgen/fields"
	flds "github.com/parsyl/parquet/cmd/parquetgen/fields"
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
		Parent: flds.Field{Type: typ, Children: parent.Children},
		Errors: errs,
	}, nil
}

func getChildren(parent *flds.Field, fields map[string]flds.Field) []error {
	var children []flds.Field
	var errs []error
	p, ok := fields[parent.Type]
	if !ok {
		errs = append(errs, fmt.Errorf("could not find %s", parent.Type))
	}

	for _, child := range p.Children {
		if child.Primitive() {
			children = append(children, child)
			continue
		}

		f, ok := fields[child.Type]
		if !ok {
			f, ok = fields[child.Type]
			if !ok {
				errs = append(errs, fmt.Errorf("unsupported type %+v", child.Type))
				continue
			}
		}

		errs = append(errs, getChildren(&child, fields)...)

		f.Name = child.Name
		f.Type = child.Type
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
		_, ok := n.(*ast.TypeSpec)
		if !ok {
			continue
		}

		parent := flds.Field{
			Type: k,
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

	rt := fields.Required
	if repeated {
		rt = fields.Repeated
	} else if optional {
		rt = fields.Optional
	}

	return flds.Field{
		Type:           typ,
		Name:           name,
		ColumnName:     tag,
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

var types = map[string]bool{
	"int32":   true,
	"uint32":  true,
	"int64":   true,
	"uint64":  true,
	"float32": true,
	"float64": true,
	"bool":    true,
	"string":  true,
}
