package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/parsyl/parquet/internal/parse"
)

var (
	typ    = flag.String("type", "", "type name")
	pkg    = flag.String("package", "", "package name")
	imp    = flag.String("import", "", "the type's import statement (only if it doesn't live in 'package')")
	pth    = flag.String("input", "", "path to the go file that defines -type")
	ignore = flag.Bool("ignore", true, "ignore unsupported fields in -type")
)

var funcs = template.FuncMap{
	"removeStar": func(s string) string {
		return strings.Replace(s, "*", "", 1)
	},
}

type fieldType struct {
	name string
	tpl  string
}

func main() {
	flag.Parse()

	result, err := parse.Fields(*typ, *pth)
	if err != nil {
		log.Fatal(err)
	}

	i := input{
		Package: *pkg,
		Type:    *typ,
		Import:  getImport(*imp),
		Fields:  result.Fields,
	}

	for _, err := range result.Errors {
		log.Println(err)
	}

	if len(result.Errors) > 0 && !*ignore {
		log.Fatal("not generating parquet.go (-ignore set to false), err: ", result.Errors)
	}

	tmpl, err := template.New("output").Parse(tpl)
	if err != nil {
		log.Fatal(err)
	}

	tmpl.Funcs(funcs)

	for _, t := range []string{
		requiredTpl,
		optionalTpl,
		stringTpl,
		stringOptionalTpl,
		boolTpl,
		boolOptionalTpl,
		newFieldTpl,
	} {
		var err error
		tmpl, err = tmpl.Parse(t)
		if err != nil {
			log.Fatal(err)
		}
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, i)
	if err != nil {
		log.Fatal(err)
	}

	gocode, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("parquet.go")
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.Write(gocode)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()
}

func getImport(i string) string {
	if i == "" {
		return ""
	}
	return fmt.Sprintf(`"%s"`, i)
}

type input struct {
	Package string
	Type    string
	Import  string
	Fields  []parse.Field
}

var newFieldTpl = `{{define "newField"}}New{{.FieldType}}(func(x {{.Type}}) {{.TypeName}} { return x.{{.FieldName}} }, func(x *{{.Type}}, v {{.TypeName}}) { x.{{.FieldName}} = v }, "{{.ColumnName}}"),{{end}}`

var optionalTpl = `{{define "optionalField"}}
type {{.FieldType}} struct {
	parquet.OptionalField
	vals []{{removeStar .TypeName}}
	read func(r *{{.Type}}, v {{.TypeName}})
	val  func(r {{.Type}}) {{.TypeName}}
}

func New{{.FieldType}}(val func(r {{.Type}}) {{.TypeName}}, read func(r *{{.Type}}, v {{.TypeName}}), col string) *{{.FieldType}} {
	return &{{.FieldType}}{
		val:           val,
		read:          read,
		OptionalField: parquet.NewOptionalField(col),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.{{.ParquetType}}, RepetitionType: parquet.RepetitionOptional}
}

func (f *{{.FieldType}}) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *{{.FieldType}}) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	v := make([]{{removeStar .TypeName}}, f.Values()-len(f.vals))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *{{.FieldType}}) Add(r {{.Type}}) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.Defs = append(f.Defs, 1)
	} else {
		f.Defs = append(f.Defs, 0)
	}
}

func (f *{{.FieldType}}) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	var val {{removeStar .TypeName}}
	if f.Defs[0] == 1 {
		v := f.vals[0]
		f.vals = f.vals[1:]
		val = v
	}
	f.Defs = f.Defs[1:]
	f.read(r, &val)
}
{{end}}`

var requiredTpl = `{{define "requiredField"}}
type {{.FieldType}} struct {
	vals []{{.TypeName}}
	parquet.RequiredField
	val  func(r {{.Type}}) {{.TypeName}}
	read func(r *{{.Type}}, v {{.TypeName}})
}

func New{{.FieldType}}(val func(r {{.Type}}) {{.TypeName}}, read func(r *{{.Type}}, v {{.TypeName}}), col string) *{{.FieldType}} {
	return &{{.FieldType}}{
		val:           val,
		read:          read,
		RequiredField: parquet.NewRequiredField(col),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.{{.ParquetType}}, RepetitionType: parquet.RepetitionRequired}
}

func (f *{{.FieldType}}) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}
	v := f.vals[0]
	f.vals = f.vals[1:]
	f.read(r, v)
}

func (f *{{.FieldType}}) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *{{.FieldType}}) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	v := make([]{{.TypeName}}, int(pos.N))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *{{.FieldType}}) Add(r {{.Type}}) {
	f.vals = append(f.vals, f.val(r))
}
{{end}}`

var stringTpl = `{{define "stringField"}}
type StringField struct {
	parquet.RequiredField
	vals []string
	val  func(r {{.Type}}) string
	read func(r {{.Type}}, v string)
}

func NewStringField(val func(r {{.Type}}) string, read func(r {{.Type}}, v string), col string) *StringField {
	return &StringField{
		val:           val,
		read:          read,
		RequiredField: parquet.NewRequiredField(col),
	}
}

func (f *StringField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.StringType, RepetitionType: parquet.RepetitionRequired}
}

func (f *StringField) Scan(r {{.Type}}) {
	if len(f.vals) == 0 {
		return
	}

	v := f.vals[0]
	f.vals = f.vals[1:]
	f.read(r, v)
}

func (f *StringField) Add(r {{.Type}}) {
	f.vals = append(f.vals, f.val(r))
}

func (f *StringField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *StringField) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	for j := 0; j < pos.N; j++ {
		var x int32
		if err := binary.Read(rr, binary.LittleEndian, &x); err != nil {
			return err
		}
		s := make([]byte, x)
		if _, err := rr.Read(s); err != nil {
			return err
		}

		f.vals = append(f.vals, string(s))
	}
	return nil
}
{{end}}`

var stringOptionalTpl = `{{define "stringOptionalField"}}
type StringOptionalField struct {
	parquet.OptionalField
	vals []string
	val  func(r {{.Type}}) *string
	read func(r *{{.Type}}, v *string)
}

func NewStringOptionalField(val func(r {{.Type}}) *string, read func(r *{{.Type}}, v *string), col string) *StringOptionalField {
	return &StringOptionalField{
		val:           val,
		read:          read,
		OptionalField: parquet.NewOptionalField(col),
	}
}

func (f *StringOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.StringType, RepetitionType: parquet.RepetitionOptional}
}

func (f *StringOptionalField) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	var val *string
	if f.Defs[0] == 1 {
		v := f.vals[0]
		f.vals = f.vals[1:]
		val = &v
	}
	f.Defs = f.Defs[1:]
	f.read(r, val)
}

func (f *StringOptionalField) Add(r {{.Type}}) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.Defs = append(f.Defs, 1)
	} else {
		f.Defs = append(f.Defs, 0)
	}
}

func (f *StringOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *StringOptionalField) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	start := len(f.Defs)
	rr, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	for j := 0; j < pos.N; j++ {
		if f.Defs[start+j] == 0 {
			continue
		}

		var x int32
		if err := binary.Read(rr, binary.LittleEndian, &x); err != nil {
			return err
		}
		s := make([]byte, x)
		if _, err := rr.Read(s); err != nil {
			return err
		}

		f.vals = append(f.vals, string(s))
	}
	return nil
}
{{end}}`

var boolTpl = `{{define "boolField"}}type BoolField struct {
	parquet.RequiredField
	vals []bool
	val  func(r {{.Type}}) bool
	read func(r *{{.Type}}, v bool)
}

func NewBoolField(val func(r {{.Type}}) bool, read func(r *{{.Type}}, v bool), col string) *BoolField {
	return &BoolField{
		val:           val,
		read:          read,
		RequiredField: parquet.NewRequiredField(col),
	}
}

func (f *BoolField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.BoolType, RepetitionType: parquet.RepetitionRequired}
}

func (f *BoolField) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}

	v := f.vals[0]
	f.vals = f.vals[1:]
	f.read(r, v)
}

func (f *BoolField) Add(r {{.Type}}) {
	f.vals = append(f.vals, f.val(r))
}

func (f *BoolField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals))
}

func (f *BoolField) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	f.vals, err = parquet.GetBools(rr, int(pos.N))
	return err
}
{{end}}`

var boolOptionalTpl = `{{define "boolOptionalField"}}type BoolOptionalField struct {
	parquet.OptionalField
	vals []bool
	val  func(r {{.Type}}) *bool
	read func(r *{{.Type}}, v *bool)
}

func NewBoolOptionalField(val func(r {{.Type}}) *bool, read func(r *{{.Type}}, v *bool), col string) *BoolOptionalField {
	return &BoolOptionalField{
		val:           val,
		read:          read,
		OptionalField: parquet.NewOptionalField(col),
	}
}

func (f *BoolOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.BoolType, RepetitionType: parquet.RepetitionOptional}
}

func (f *BoolOptionalField) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	v, err := parquet.GetBools(rr, f.Values()-len(f.vals))
	f.vals = append(f.vals, v...)
	return err
}

func (f *BoolOptionalField) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	var val *bool
	if f.Defs[0] == 1 {
		v := f.vals[0]
		f.vals = f.vals[1:]
		val = &v
	}
	f.Defs = f.Defs[1:]
	f.read(r, val)
}

func (f *BoolOptionalField) Add(r {{.Type}}) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.Defs = append(f.Defs, 1)
	} else {
		f.Defs = append(f.Defs, 0)
	}
}

func (f *BoolOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals))
}
{{end}}`

var tpl = `package {{.Package}}

// This code is generated by github.com/parsyl/parquet.

import (
	"fmt"
	"io"
	"bytes"
	"encoding/binary"

	"github.com/parsyl/parquet"
	{{.Import}}
)

// ParquetWriter reprents a row group
type ParquetWriter struct {
	fields []Field

	len int

	// child points to the next page
	child *ParquetWriter

	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta *parquet.Metadata
	w    *parquet.WriteCounter
}

func Fields() []Field {
	return []Field{ {{range .Fields}}
		{{template "newField" .}}{{end}}
	}
}

func NewParquetWriter(w io.Writer, opts ...func(*ParquetWriter) error) (*ParquetWriter, error) {
	return newParquetWriter(w, append(opts, begin)...)
}

func newParquetWriter(w io.Writer, opts ...func(*ParquetWriter) error) (*ParquetWriter, error) {
	p := &ParquetWriter{
		max:    1000,
		w:      parquet.NewWriteCounter(w),
		fields: Fields(),
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	if p.meta == nil {
		ff := Fields()
		schema := make([]parquet.Field, len(ff))
		for i, f := range ff {
			schema[i] = f.Schema()
		}
		p.meta = parquet.New(schema...)
	}

	return p, nil
}

// MaxPageSize is the maximum number of rows in each row groups' page.
func MaxPageSize(m int) func(*ParquetWriter) error {
	return func(p *ParquetWriter) error {
		p.max = m
		return nil
	}
}

func begin(p *ParquetWriter) error {
	_, err := p.w.Write([]byte("PAR1"))
	return err
}

func withMeta(m *parquet.Metadata) func(*ParquetWriter) error {
	return func(p *ParquetWriter) error {
		p.meta = m
		return nil
	}
}

func (p *ParquetWriter) Write() error {
	for i, f := range p.fields {
		if err := f.Write(p.w, p.meta); err != nil {
			return err
		}

		for child := p.child; child != nil; child = child.child {
			if err := child.fields[i].Write(p.w, p.meta); err != nil {
				return err
			}
		}
	}

	p.fields = Fields()
	p.child = nil
	p.len = 0

	schema := make([]parquet.Field, len(p.fields))
	for i, f := range p.fields {
		schema[i] = f.Schema()
	}
	p.meta.StartRowGroup(schema...)
	return nil
}

func (p *ParquetWriter) Close() error {
	if err := p.meta.Footer(p.w); err != nil {
		return err
	}

	_, err := p.w.Write([]byte("PAR1"))
	return err
}

func (p *ParquetWriter) Add(rec {{.Type}}) {
	if p.len == p.max {
		if p.child == nil {
			// an error can't happen here
			p.child, _ = newParquetWriter(p.w, MaxPageSize(p.max), withMeta(p.meta))
		}

		p.child.Add(rec)
		return
	}

	for _, f := range p.fields {
		f.Add(rec)
	}

	p.len++
}

type Field interface {
	Add(r {{.Type}})
	Write(w io.Writer, meta *parquet.Metadata) error
	Schema() parquet.Field
	Scan(r *{{.Type}})
	Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error
	Name() string
}

func getFields(ff []Field) map[string]Field {
	m := make(map[string]Field, len(ff))
	for _, f := range ff {
		m[f.Name()] = f
	}
	return m
}

func NewParquetReader(r io.ReadSeeker, opts ...func(*ParquetReader)) (*ParquetReader, error) {
	ff := Fields()
	pr := &ParquetReader{
		r: r,
	}

	for _, opt := range opts {
		opt(pr)
	}

	schema := make([]parquet.Field, len(ff))
	for i, f := range ff {
		schema[i] = f.Schema()
	}

	meta := parquet.New(schema...)
	if err := meta.ReadFooter(r); err != nil {
		return nil, err
	}
	pr.rows = meta.Rows()
	var err error
	pr.offsets, err = meta.Offsets()
	if err != nil {
		return nil, err
	}

	pr.rowGroups = meta.RowGroups()
	_, err = r.Seek(4, io.SeekStart)
	if err != nil {
		return nil, err
	}
	pr.meta = meta

	return pr, pr.readRowGroup()
}

func readerIndex(i int) func(*ParquetReader) {
	return func(p *ParquetReader) {
		p.index = i
	}
}

// ParquetReader reads one page from a row group.
type ParquetReader struct {
	fields         map[string]Field
	index          int
	cursor         int64
	rows           int64
	rowGroupCursor int64
	rowGroupCount  int64
	offsets        map[string][]parquet.Position
	meta           *parquet.Metadata
	err            error

	r         io.ReadSeeker
	rowGroups []parquet.RowGroup
}

func (p *ParquetReader) Error() error {
	return p.err
}

func (p *ParquetReader) readRowGroup() error {
	rg := p.rowGroups[0]
	p.fields = getFields(Fields())
	p.rowGroupCount = rg.Rows
	p.rowGroupCursor = 0
	for _, col := range rg.Columns() {
		name := col.MetaData.PathInSchema[len(col.MetaData.PathInSchema)-1]
		f, ok := p.fields[name]
		if !ok {
			return fmt.Errorf("unknown field: %s", name)
		}
		offsets := p.offsets[f.Name()]
		if len(offsets) <= p.index {
			break
		}

		o := offsets[0]
		if err := f.Read(p.r, p.meta, o); err != nil {
			return fmt.Errorf("unable to read field %s, err: %s", f.Name(), err)
		}
		p.offsets[f.Name()] = p.offsets[f.Name()][1:]
	}
	p.rowGroups = p.rowGroups[1:]
	return nil
}

func (p *ParquetReader) Rows() int64 {
	return p.rows
}

func (p *ParquetReader) Next() bool {
	if p.err == nil && p.cursor >= p.rows {
		return false
	}
	if p.rowGroupCursor >= p.rowGroupCount {
		p.err = p.readRowGroup()
		if p.err != nil {
			return false
		}
	}

	p.cursor++
	p.rowGroupCursor++
	return true
}

func (p *ParquetReader) Scan(x *{{.Type}}) {
	if p.err != nil {
		return
	}

	for _, f := range p.fields {
		f.Scan(x)
	}
}

{{range .Fields}}
{{if eq .Category "numeric"}}
{{ template "requiredField" .}}
{{end}}
{{if eq .Category "numericOptional"}}
{{ template "optionalField" .}}
{{end}}
{{if eq .Category "string"}}
{{ template "stringField" .}}
{{end}}
{{if eq .Category "stringOptional"}}
{{ template "stringOptionalField" .}}
{{end}}
{{if eq .Category "bool"}}
{{ template "boolField" .}}
{{end}}
{{if eq .Category "boolOptional"}}
{{ template "boolOptionalField" .}}
{{end}}
{{end}}
`
