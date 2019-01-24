package main

import (
	"flag"
	"log"
	"os"
	"text/template"
)

var (
	typ = flag.String("type", "", "type name")
	pkg = flag.String("package", "", "package name")
)

func main() {
	flag.Parse()

	i := Input{
		Package: *pkg,
		Type:    *typ,
	}

	tmpl, err := template.New("output").Parse(tpl)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("parquet.go")
	if err != nil {
		log.Fatal(err)
	}

	err = tmpl.Execute(f, i)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()
}

type Input struct {
	Package string
	Type    string
}

var tpl = `package {{.Package}}

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/parsyl/parquet"
	"github.com/golang/snappy"
)

// Records reprents a row group
type Records struct {
	fields []Field

	len int

	// records are for subsequent chunks
	records *Records

	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta *parquet.Metadata
	w    *WriteCounter
}

func NewParquetWriter(w io.Writer, fields []Field, meta *parquet.Metadata, opts ...func(*Records)) *Records {
	r := &Records{
		max:    1000,
		w:      &WriteCounter{w: w},
		fields: fields,
		meta:   meta,
	}

	for _, opt := range opts {
		opt(r)
	}
	return r
}

// MaxPageSize is the maximum number of rows in each row groups' page.
func MaxPageSize(m int) func(*Records) {
	return func(r *Records) {
		r.max = m
	}
}

func (r *Records) Write() error {
	if _, err := r.w.Write([]byte("PAR1")); err != nil {
		return err
	}

	for i, f := range r.fields {
		pos := r.w.n
		f.Write(r.w, r.meta, pos)

		for child := r.records; child != nil; child = child.records {
			pos := r.w.n
			child.fields[i].Write(r.w, r.meta, pos)
		}
	}

	if err := r.meta.Footer(r.w); err != nil {
		return err
	}

	_, err := r.w.Write([]byte("PAR1"))
	return err
}

func (r *Records) Add(rec {{.Type}}) {
	if r.len == r.max {
		if r.records == nil {
			r.records = NewParquetWriter(r.w, r.fields, r.meta, MaxPageSize(r.max))
			r.records.meta = r.meta
		}

		r.records.Add(rec)
		return
	}

	for _, f := range r.fields {
		f.Add(rec)
	}

	r.len++
}

type Field interface {
	Add(r {{.Type}})
	Write(w io.Writer, meta *parquet.Metadata, pos int) error
	Schema() parquet.Field
}

type RequiredNumField struct {
	vals []interface{}
	col  string
}

func (i *RequiredNumField) Write(w io.Writer, meta *parquet.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &WriteCounter{w: &buf}

	for _, i := range i.vals {
		if err := binary.Write(wc, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, i.col, pos, wc.n, len(compressed), len(i.vals)); err != nil {
		return err
	}

	_, err := io.Copy(w, bytes.NewBuffer(compressed))
	return err
}

type OptionalNumField struct {
	vals []interface{}
	defs []int64
	col  string
}

func (i *OptionalNumField) Write(w io.Writer, meta *parquet.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &WriteCounter{w: &buf}

	err := WriteLevels(wc, i.defs)
	if err != nil {
		return err
	}

	for _, i := range i.vals {
		if err := binary.Write(wc, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, i.col, pos, wc.n, len(compressed), len(i.defs)); err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(compressed))
	return err
}

type Int32Field struct {
	RequiredNumField
	val func(r {{.Type}}) int32
	col string
}

func NewInt32Field(val func(r {{.Type}}) int32, col string) *Int32Field {
	return &Int32Field{
		col:              col,
		val:              val,
		RequiredNumField: RequiredNumField{col: col},
	}
}

func (i *Int32Field) Schema() parquet.Field {
	return parquet.Field{Name: i.col, Type: parquet.Int32Type, RepetitionType: parquet.RepetitionRequired}
}

func (i *Int32Field) Add(r {{.Type}}) {
	i.vals = append(i.vals, i.val(r))
}

type Int32OptionalField struct {
	OptionalNumField
	val func(r {{.Type}}) *int32
	col string
}

func NewInt32OptionalField(val func(r {{.Type}}) *int32, col string) *Int32OptionalField {
	return &Int32OptionalField{
		col:              col,
		val:              val,
		OptionalNumField: OptionalNumField{col: col},
	}
}

func (i *Int32OptionalField) Schema() parquet.Field {
	return parquet.Field{Name: i.col, Type: parquet.Int32Type, RepetitionType: parquet.RepetitionOptional}
}

func (i *Int32OptionalField) Add(r {{.Type}}) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type Int64Field struct {
	RequiredNumField
	val func(r {{.Type}}) int64
	col string
}

func NewInt64Field(val func(r {{.Type}}) int64, col string) *Int64Field {
	return &Int64Field{
		col:              col,
		val:              val,
		RequiredNumField: RequiredNumField{col: col},
	}
}

func (i *Int64Field) Schema() parquet.Field {
	return parquet.Field{Name: i.col, Type: parquet.Int64Type, RepetitionType: parquet.RepetitionRequired}
}

func (i *Int64Field) Add(r {{.Type}}) {
	i.vals = append(i.vals, i.val(r))
}

type Int64OptionalField struct {
	OptionalNumField
	val func(r {{.Type}}) *int64
	col string
}

func NewInt64OptionalField(val func(r {{.Type}}) *int64, col string) *Int64OptionalField {
	return &Int64OptionalField{
		col:              col,
		val:              val,
		OptionalNumField: OptionalNumField{col: col},
	}
}

func (i *Int64OptionalField) Schema() parquet.Field {
	return parquet.Field{Name: i.col, Type: parquet.Int64Type, RepetitionType: parquet.RepetitionOptional}
}

func (i *Int64OptionalField) Add(r {{.Type}}) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type Float32Field struct {
	RequiredNumField
	val func(r {{.Type}}) float32
	col string
}

func NewFloat32Field(val func(r {{.Type}}) float32, col string) *Float32Field {
	return &Float32Field{
		col:              col,
		val:              val,
		RequiredNumField: RequiredNumField{col: col},
	}
}

func (i *Float32Field) Schema() parquet.Field {
	return parquet.Field{Name: i.col, Type: parquet.Float32Type, RepetitionType: parquet.RepetitionRequired}
}

func (i *Float32Field) Add(r {{.Type}}) {
	i.vals = append(i.vals, i.val(r))
}

type Float32OptionalField struct {
	OptionalNumField
	val func(r {{.Type}}) *float32
	col string
}

func NewFloat32OptionalField(val func(r {{.Type}}) *float32, col string) *Float32OptionalField {
	return &Float32OptionalField{
		col:              col,
		val:              val,
		OptionalNumField: OptionalNumField{col: col},
	}
}

func (i *Float32OptionalField) Schema() parquet.Field {
	return parquet.Field{Name: i.col, Type: parquet.Float32Type, RepetitionType: parquet.RepetitionOptional}
}

func (i *Float32OptionalField) Add(r {{.Type}}) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)

		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type BoolOptionalField struct {
	vals []bool
	defs []int64
	col  string
	val  func(r {{.Type}}) *bool
}

func NewBoolOptionalField(val func(r {{.Type}}) *bool, col string) *BoolOptionalField {
	return &BoolOptionalField{
		val: val,
		col: col,
	}
}

func (f *BoolOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.col, Type: parquet.BoolType, RepetitionType: parquet.RepetitionOptional}
}

func (f *BoolOptionalField) Add(r {{.Type}}) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.defs = append(f.defs, 1)
	} else {
		f.defs = append(f.defs, 0)
	}
}

func (f *BoolOptionalField) Write(w io.Writer, meta *parquet.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &WriteCounter{w: &buf}

	err := WriteLevels(wc, f.defs)
	if err != nil {
		return err
	}

	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	wc.Write(rawBuf)

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, f.col, pos, wc.n, len(compressed), len(f.defs)); err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(compressed))
	return err
}

type StringField struct {
	vals []string
	col  string
	val  func(r {{.Type}}) string
}

func NewStringField(val func(r {{.Type}}) string, col string) *StringField {
	return &StringField{
		val: val,
		col: col,
	}
}

func (f *StringField) Schema() parquet.Field {
	return parquet.Field{Name: f.col, Type: parquet.StringType, RepetitionType: parquet.RepetitionRequired}
}

func (f *StringField) Add(r {{.Type}}) {
	f.vals = append(f.vals, f.val(r))
}

func (f *StringField) Write(w io.Writer, meta *parquet.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &WriteCounter{w: &buf}

	for _, s := range f.vals {
		if err := binary.Write(wc, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		wc.Write([]byte(s))
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, f.col, pos, wc.n, len(compressed), len(f.vals)); err != nil {
		return err
	}

	_, err := io.Copy(w, bytes.NewBuffer(compressed))
	return err
}

type WriteCounter struct {
	n int
	w io.Writer
}

func (w *WriteCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += n
	return n, err
}

// WriteLevels writes vals to w as RLE encoded data
func WriteLevels(w io.Writer, vals []int64) error {
	var max uint64
	if len(vals) > 0 {
		max = 1
	}

	rleBuf := writeRLE(vals, int32(bitNum(max)))
	res := make([]byte, 0)
	var lenBuf bytes.Buffer
	binary.Write(&lenBuf, binary.LittleEndian, int32(len(rleBuf)))
	res = append(res, lenBuf.Bytes()...)
	res = append(res, rleBuf...)
	_, err := io.Copy(w, bytes.NewBuffer(res))
	return err
}

func writeRLE(vals []int64, bitWidth int32) []byte {
	ln := len(vals)
	i := 0
	res := make([]byte, 0)
	for i < ln {
		j := i + 1
		for j < ln && vals[j] == vals[i] {
			j++
		}
		num := j - i
		header := num << 1
		byteNum := (bitWidth + 7) / 8

		headerBuf := writeUnsignedVarInt(uint64(header))

		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, vals[i])
		valBuf := buf.Bytes()
		rleBuf := make([]byte, int64(len(headerBuf))+int64(byteNum))
		copy(rleBuf[0:], headerBuf)
		copy(rleBuf[len(headerBuf):], valBuf[0:byteNum])
		res = append(res, rleBuf...)
		i = j
	}
	return res
}

func writeUnsignedVarInt(num uint64) []byte {
	byteNum := (bitNum(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
	}
	res := make([]byte, byteNum)

	numTmp := num
	for i := 0; i < int(byteNum); i++ {
		res[i] = byte(numTmp & uint64(0x7F))
		res[i] = res[i] | byte(0x80)
		numTmp = numTmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}

func bitNum(num uint64) uint64 {
	var bitn uint64 = 0
	for ; num != 0; num >>= 1 {
		bitn++
	}
	return bitn
}`
