package performance

// Code generated by github.com/parsyl/parquet.  DO NOT EDIT.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/parsyl/parquet"
	. "github.com/parsyl/parquet/performance/message"
	sch "github.com/parsyl/parquet/schema"
	"github.com/valyala/bytebufferpool"
	"math"
	"sort"
)

type compression int

const (
	compressionUncompressed compression = 0
	compressionSnappy       compression = 1
	compressionGzip         compression = 2
	compressionUnknown      compression = -1
)

var buffpool = bytebufferpool.Pool{}

// ParquetWriter reprents a row group
type ParquetWriter struct {
	fields []Field

	len int

	// child points to the next page
	child *ParquetWriter

	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta        *parquet.Metadata
	w           io.Writer
	compression compression
}

func Fields(compression compression) []Field {
	return []Field{
		NewStringField(readColStr0, writeColStr0, []string{"col_str_0"}, fieldCompression(compression)),
		NewStringField(readColStr1, writeColStr1, []string{"col_str_1"}, fieldCompression(compression)),
		NewStringField(readColStr2, writeColStr2, []string{"col_str_2"}, fieldCompression(compression)),
		NewStringField(readColStr3, writeColStr3, []string{"col_str_3"}, fieldCompression(compression)),
		NewStringField(readColStr4, writeColStr4, []string{"col_str_4"}, fieldCompression(compression)),
		NewStringField(readColStr5, writeColStr5, []string{"col_str_5"}, fieldCompression(compression)),
		NewStringField(readColStr6, writeColStr6, []string{"col_str_6"}, fieldCompression(compression)),
		NewStringField(readColStr7, writeColStr7, []string{"col_str_7"}, fieldCompression(compression)),
		NewStringField(readColStr8, writeColStr8, []string{"col_str_8"}, fieldCompression(compression)),
		NewStringField(readColStr9, writeColStr9, []string{"col_str_9"}, fieldCompression(compression)),
		NewInt64Field(readColInt0, writeColInt0, []string{"col_int_0"}, fieldCompression(compression)),
		NewInt64Field(readColInt1, writeColInt1, []string{"col_int_1"}, fieldCompression(compression)),
		NewInt64Field(readColInt2, writeColInt2, []string{"col_int_2"}, fieldCompression(compression)),
		NewInt64Field(readColInt3, writeColInt3, []string{"col_int_3"}, fieldCompression(compression)),
		NewInt64Field(readColInt4, writeColInt4, []string{"col_int_4"}, fieldCompression(compression)),
		NewInt64Field(readColInt5, writeColInt5, []string{"col_int_5"}, fieldCompression(compression)),
		NewInt64Field(readColInt6, writeColInt6, []string{"col_int_6"}, fieldCompression(compression)),
		NewInt64Field(readColInt7, writeColInt7, []string{"col_int_7"}, fieldCompression(compression)),
		NewInt64Field(readColInt8, writeColInt8, []string{"col_int_8"}, fieldCompression(compression)),
		NewInt64Field(readColInt9, writeColInt9, []string{"col_int_9"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat0, writeColFloat0, []string{"col_float_0"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat1, writeColFloat1, []string{"col_float_1"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat2, writeColFloat2, []string{"col_float_2"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat3, writeColFloat3, []string{"col_float_3"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat4, writeColFloat4, []string{"col_float_4"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat5, writeColFloat5, []string{"col_float_5"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat6, writeColFloat6, []string{"col_float_6"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat7, writeColFloat7, []string{"col_float_7"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat8, writeColFloat8, []string{"col_float_8"}, fieldCompression(compression)),
		NewFloat64Field(readColFloat9, writeColFloat9, []string{"col_float_9"}, fieldCompression(compression)),
		NewBoolField(readColBool0, writeColBool0, []string{"col_bool_0"}, fieldCompression(compression)),
		NewBoolField(readColBool1, writeColBool1, []string{"col_bool_1"}, fieldCompression(compression)),
		NewBoolField(readColBool2, writeColBool2, []string{"col_bool_2"}, fieldCompression(compression)),
		NewBoolField(readColBool3, writeColBool3, []string{"col_bool_3"}, fieldCompression(compression)),
		NewBoolField(readColBool4, writeColBool4, []string{"col_bool_4"}, fieldCompression(compression)),
		NewBoolField(readColBool5, writeColBool5, []string{"col_bool_5"}, fieldCompression(compression)),
		NewBoolField(readColBool6, writeColBool6, []string{"col_bool_6"}, fieldCompression(compression)),
		NewBoolField(readColBool7, writeColBool7, []string{"col_bool_7"}, fieldCompression(compression)),
		NewBoolField(readColBool8, writeColBool8, []string{"col_bool_8"}, fieldCompression(compression)),
		NewBoolField(readColBool9, writeColBool9, []string{"col_bool_9"}, fieldCompression(compression)),
	}
}

func readColStr0(x Message) string {
	return x.ColStr0
}

func writeColStr0(x *Message, vals []string) {
	x.ColStr0 = vals[0]
}

func readColStr1(x Message) string {
	return x.ColStr1
}

func writeColStr1(x *Message, vals []string) {
	x.ColStr1 = vals[0]
}

func readColStr2(x Message) string {
	return x.ColStr2
}

func writeColStr2(x *Message, vals []string) {
	x.ColStr2 = vals[0]
}

func readColStr3(x Message) string {
	return x.ColStr3
}

func writeColStr3(x *Message, vals []string) {
	x.ColStr3 = vals[0]
}

func readColStr4(x Message) string {
	return x.ColStr4
}

func writeColStr4(x *Message, vals []string) {
	x.ColStr4 = vals[0]
}

func readColStr5(x Message) string {
	return x.ColStr5
}

func writeColStr5(x *Message, vals []string) {
	x.ColStr5 = vals[0]
}

func readColStr6(x Message) string {
	return x.ColStr6
}

func writeColStr6(x *Message, vals []string) {
	x.ColStr6 = vals[0]
}

func readColStr7(x Message) string {
	return x.ColStr7
}

func writeColStr7(x *Message, vals []string) {
	x.ColStr7 = vals[0]
}

func readColStr8(x Message) string {
	return x.ColStr8
}

func writeColStr8(x *Message, vals []string) {
	x.ColStr8 = vals[0]
}

func readColStr9(x Message) string {
	return x.ColStr9
}

func writeColStr9(x *Message, vals []string) {
	x.ColStr9 = vals[0]
}

func readColInt0(x Message) int64 {
	return x.ColInt0
}

func writeColInt0(x *Message, vals []int64) {
	x.ColInt0 = vals[0]
}

func readColInt1(x Message) int64 {
	return x.ColInt1
}

func writeColInt1(x *Message, vals []int64) {
	x.ColInt1 = vals[0]
}

func readColInt2(x Message) int64 {
	return x.ColInt2
}

func writeColInt2(x *Message, vals []int64) {
	x.ColInt2 = vals[0]
}

func readColInt3(x Message) int64 {
	return x.ColInt3
}

func writeColInt3(x *Message, vals []int64) {
	x.ColInt3 = vals[0]
}

func readColInt4(x Message) int64 {
	return x.ColInt4
}

func writeColInt4(x *Message, vals []int64) {
	x.ColInt4 = vals[0]
}

func readColInt5(x Message) int64 {
	return x.ColInt5
}

func writeColInt5(x *Message, vals []int64) {
	x.ColInt5 = vals[0]
}

func readColInt6(x Message) int64 {
	return x.ColInt6
}

func writeColInt6(x *Message, vals []int64) {
	x.ColInt6 = vals[0]
}

func readColInt7(x Message) int64 {
	return x.ColInt7
}

func writeColInt7(x *Message, vals []int64) {
	x.ColInt7 = vals[0]
}

func readColInt8(x Message) int64 {
	return x.ColInt8
}

func writeColInt8(x *Message, vals []int64) {
	x.ColInt8 = vals[0]
}

func readColInt9(x Message) int64 {
	return x.ColInt9
}

func writeColInt9(x *Message, vals []int64) {
	x.ColInt9 = vals[0]
}

func readColFloat0(x Message) float64 {
	return x.ColFloat0
}

func writeColFloat0(x *Message, vals []float64) {
	x.ColFloat0 = vals[0]
}

func readColFloat1(x Message) float64 {
	return x.ColFloat1
}

func writeColFloat1(x *Message, vals []float64) {
	x.ColFloat1 = vals[0]
}

func readColFloat2(x Message) float64 {
	return x.ColFloat2
}

func writeColFloat2(x *Message, vals []float64) {
	x.ColFloat2 = vals[0]
}

func readColFloat3(x Message) float64 {
	return x.ColFloat3
}

func writeColFloat3(x *Message, vals []float64) {
	x.ColFloat3 = vals[0]
}

func readColFloat4(x Message) float64 {
	return x.ColFloat4
}

func writeColFloat4(x *Message, vals []float64) {
	x.ColFloat4 = vals[0]
}

func readColFloat5(x Message) float64 {
	return x.ColFloat5
}

func writeColFloat5(x *Message, vals []float64) {
	x.ColFloat5 = vals[0]
}

func readColFloat6(x Message) float64 {
	return x.ColFloat6
}

func writeColFloat6(x *Message, vals []float64) {
	x.ColFloat6 = vals[0]
}

func readColFloat7(x Message) float64 {
	return x.ColFloat7
}

func writeColFloat7(x *Message, vals []float64) {
	x.ColFloat7 = vals[0]
}

func readColFloat8(x Message) float64 {
	return x.ColFloat8
}

func writeColFloat8(x *Message, vals []float64) {
	x.ColFloat8 = vals[0]
}

func readColFloat9(x Message) float64 {
	return x.ColFloat9
}

func writeColFloat9(x *Message, vals []float64) {
	x.ColFloat9 = vals[0]
}

func readColBool0(x Message) bool {
	return x.ColBool0
}

func writeColBool0(x *Message, vals []bool) {
	x.ColBool0 = vals[0]
}

func readColBool1(x Message) bool {
	return x.ColBool1
}

func writeColBool1(x *Message, vals []bool) {
	x.ColBool1 = vals[0]
}

func readColBool2(x Message) bool {
	return x.ColBool2
}

func writeColBool2(x *Message, vals []bool) {
	x.ColBool2 = vals[0]
}

func readColBool3(x Message) bool {
	return x.ColBool3
}

func writeColBool3(x *Message, vals []bool) {
	x.ColBool3 = vals[0]
}

func readColBool4(x Message) bool {
	return x.ColBool4
}

func writeColBool4(x *Message, vals []bool) {
	x.ColBool4 = vals[0]
}

func readColBool5(x Message) bool {
	return x.ColBool5
}

func writeColBool5(x *Message, vals []bool) {
	x.ColBool5 = vals[0]
}

func readColBool6(x Message) bool {
	return x.ColBool6
}

func writeColBool6(x *Message, vals []bool) {
	x.ColBool6 = vals[0]
}

func readColBool7(x Message) bool {
	return x.ColBool7
}

func writeColBool7(x *Message, vals []bool) {
	x.ColBool7 = vals[0]
}

func readColBool8(x Message) bool {
	return x.ColBool8
}

func writeColBool8(x *Message, vals []bool) {
	x.ColBool8 = vals[0]
}

func readColBool9(x Message) bool {
	return x.ColBool9
}

func writeColBool9(x *Message, vals []bool) {
	x.ColBool9 = vals[0]
}

func fieldCompression(c compression) func(*parquet.RequiredField) {
	switch c {
	case compressionUncompressed:
		return parquet.RequiredFieldUncompressed
	case compressionSnappy:
		return parquet.RequiredFieldSnappy
	case compressionGzip:
		return parquet.RequiredFieldGzip
	default:
		return parquet.RequiredFieldUncompressed
	}
}

func optionalFieldCompression(c compression) func(*parquet.OptionalField) {
	switch c {
	case compressionUncompressed:
		return parquet.OptionalFieldUncompressed
	case compressionSnappy:
		return parquet.OptionalFieldSnappy
	case compressionGzip:
		return parquet.OptionalFieldGzip
	default:
		return parquet.OptionalFieldUncompressed
	}
}

func NewParquetWriter(w io.Writer, opts ...func(*ParquetWriter) error) (*ParquetWriter, error) {
	return newParquetWriter(w, append(opts, begin)...)
}

func newParquetWriter(w io.Writer, opts ...func(*ParquetWriter) error) (*ParquetWriter, error) {
	p := &ParquetWriter{
		max:         1000,
		w:           w,
		compression: compressionSnappy,
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	p.fields = Fields(p.compression)
	if p.meta == nil {
		ff := Fields(p.compression)
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

func Uncompressed(p *ParquetWriter) error {
	p.compression = compressionUncompressed
	return nil
}

func Snappy(p *ParquetWriter) error {
	p.compression = compressionSnappy
	return nil
}

func Gzip(p *ParquetWriter) error {
	p.compression = compressionGzip
	return nil
}

func withCompression(c compression) func(*ParquetWriter) error {
	return func(p *ParquetWriter) error {
		p.compression = c
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

	p.fields = Fields(p.compression)
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

func (p *ParquetWriter) Add(rec Message) {
	if p.len == p.max {
		if p.child == nil {
			// an error can't happen here
			p.child, _ = newParquetWriter(p.w, MaxPageSize(p.max), withMeta(p.meta), withCompression(p.compression))
		}

		p.child.Add(rec)
		return
	}

	p.meta.NextDoc()
	for _, f := range p.fields {
		f.Add(rec)
	}

	p.len++
}

type Field interface {
	Add(r Message)
	Write(w io.Writer, meta *parquet.Metadata) error
	Schema() parquet.Field
	Scan(r *Message)
	Read(r io.ReadSeeker, pg parquet.Page) error
	Name() string
	Levels() ([]uint8, []uint8)
}

func getFields(ff []Field) map[string]Field {
	m := make(map[string]Field, len(ff))
	for _, f := range ff {
		m[f.Name()] = f
	}
	return m
}

func NewParquetReader(r io.ReadSeeker, opts ...func(*ParquetReader)) (*ParquetReader, error) {
	ff := Fields(compressionUnknown)
	pr := &ParquetReader{
		r: r,
	}

	for _, opt := range opts {
		opt(pr)
	}

	schema := make([]parquet.Field, len(ff))
	for i, f := range ff {
		pr.fieldNames = append(pr.fieldNames, f.Name())
		schema[i] = f.Schema()
	}

	meta := parquet.New(schema...)
	if err := meta.ReadFooter(r); err != nil {
		return nil, err
	}
	pr.rows = meta.Rows()
	var err error
	pr.pages, err = meta.Pages()
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
	fieldNames     []string
	index          int
	cursor         int64
	rows           int64
	rowGroupCursor int64
	rowGroupCount  int64
	pages          map[string][]parquet.Page
	meta           *parquet.Metadata
	err            error

	r         io.ReadSeeker
	rowGroups []parquet.RowGroup
}

type Levels struct {
	Name string
	Defs []uint8
	Reps []uint8
}

func (p *ParquetReader) Levels() []Levels {
	var out []Levels
	//for {
	for _, name := range p.fieldNames {
		f := p.fields[name]
		d, r := f.Levels()
		out = append(out, Levels{Name: f.Name(), Defs: d, Reps: r})
	}
	//	if err := p.readRowGroup(); err != nil {
	//		break
	//	}
	//}
	return out
}

func (p *ParquetReader) Error() error {
	return p.err
}

func (p *ParquetReader) readRowGroup() error {
	p.rowGroupCursor = 0

	if len(p.rowGroups) == 0 {
		p.rowGroupCount = 0
		return nil
	}

	rg := p.rowGroups[0]
	p.fields = getFields(Fields(compressionUnknown))
	p.rowGroupCount = rg.Rows
	p.rowGroupCursor = 0
	for _, col := range rg.Columns() {
		name := strings.Join(col.MetaData.PathInSchema, ".")
		f, ok := p.fields[name]
		if !ok {
			return fmt.Errorf("unknown field: %s", name)
		}
		pages := p.pages[name]
		if len(pages) <= p.index {
			break
		}

		pg := pages[0]
		if err := f.Read(p.r, pg); err != nil {
			return fmt.Errorf("unable to read field %s, err: %s", f.Name(), err)
		}
		p.pages[name] = p.pages[name][1:]
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

func (p *ParquetReader) Scan(x *Message) {
	if p.err != nil {
		return
	}

	for _, name := range p.fieldNames {
		f := p.fields[name]
		f.Scan(x)
	}
}

type StringField struct {
	parquet.RequiredField
	vals  []string
	read  func(r Message) string
	write func(r *Message, vals []string)
	stats *stringStats
}

func NewStringField(read func(r Message) string, write func(r *Message, vals []string), path []string, opts ...func(*parquet.RequiredField)) *StringField {
	return &StringField{
		read:          read,
		write:         write,
		RequiredField: parquet.NewRequiredField(path, opts...),
		stats:         newStringStats(),
	}
}

func (f *StringField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: StringType, RepetitionType: parquet.RepetitionRequired, Types: []int{0}}
}

func (f *StringField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := buffpool.Get()
	defer buffpool.Put(buf)

	for _, s := range f.vals {
		if err := binary.Write(buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
}

func (f *StringField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	for j := 0; j < pg.N; j++ {
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

func (f *StringField) Scan(r *Message) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *StringField) Add(r Message) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}

func (f *StringField) Levels() ([]uint8, []uint8) {
	return nil, nil
}

type Int64Field struct {
	vals []int64
	parquet.RequiredField
	read  func(r Message) int64
	write func(r *Message, vals []int64)
	stats *int64stats
}

func NewInt64Field(read func(r Message) int64, write func(r *Message, vals []int64), path []string, opts ...func(*parquet.RequiredField)) *Int64Field {
	return &Int64Field{
		read:          read,
		write:         write,
		RequiredField: parquet.NewRequiredField(path, opts...),
		stats:         newInt64stats(),
	}
}

func (f *Int64Field) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: Int64Type, RepetitionType: parquet.RepetitionRequired, Types: []int{0}}
}

func (f *Int64Field) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v := make([]int64, int(pg.N))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *Int64Field) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := buffpool.Get()
	defer buffpool.Put(buf)

	for _, v := range f.vals {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
}

func (f *Int64Field) Scan(r *Message) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *Int64Field) Add(r Message) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}

func (f *Int64Field) Levels() ([]uint8, []uint8) {
	return nil, nil
}

type Float64Field struct {
	vals []float64
	parquet.RequiredField
	read  func(r Message) float64
	write func(r *Message, vals []float64)
	stats *float64stats
}

func NewFloat64Field(read func(r Message) float64, write func(r *Message, vals []float64), path []string, opts ...func(*parquet.RequiredField)) *Float64Field {
	return &Float64Field{
		read:          read,
		write:         write,
		RequiredField: parquet.NewRequiredField(path, opts...),
		stats:         newFloat64stats(),
	}
}

func (f *Float64Field) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: Float64Type, RepetitionType: parquet.RepetitionRequired, Types: []int{0}}
}

func (f *Float64Field) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v := make([]float64, int(pg.N))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *Float64Field) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := buffpool.Get()
	defer buffpool.Put(buf)

	for _, v := range f.vals {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
}

func (f *Float64Field) Scan(r *Message) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *Float64Field) Add(r Message) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}

func (f *Float64Field) Levels() ([]uint8, []uint8) {
	return nil, nil
}

type BoolField struct {
	parquet.RequiredField
	vals  []bool
	read  func(r Message) bool
	write func(r *Message, vals []bool)
	stats *boolStats
}

func NewBoolField(read func(r Message) bool, write func(r *Message, vals []bool), path []string, opts ...func(*parquet.RequiredField)) *BoolField {
	return &BoolField{
		read:          read,
		write:         write,
		RequiredField: parquet.NewRequiredField(path, opts...),
	}
}

func (f *BoolField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: BoolType, RepetitionType: parquet.RepetitionRequired, Types: []int{0}}
}

func (f *BoolField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	n := (ln + 7) / 8
	rawBuf := make([]byte, n)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals), newBoolStats())
}

func (f *BoolField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, sizes, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	f.vals, err = parquet.GetBools(rr, int(pg.N), sizes)
	return err
}

func (f *BoolField) Scan(r *Message) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *BoolField) Add(r Message) {
	v := f.read(r)
	f.vals = append(f.vals, v)
}

func (f *BoolField) Levels() ([]uint8, []uint8) {
	return nil, nil
}

type stringStats struct {
	vals []string
	min  []byte
	max  []byte
}

func newStringStats() *stringStats {
	return &stringStats{}
}

func (s *stringStats) add(val string) {
	s.vals = append(s.vals, val)
}

func (s *stringStats) NullCount() *int64 {
	return nil
}

func (s *stringStats) DistinctCount() *int64 {
	return nil
}

func (s *stringStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringStats) minMax() {
	if len(s.vals) == 0 {
		return
	}

	tmp := make([]string, len(s.vals))
	copy(tmp, s.vals)
	sort.Strings(tmp)
	s.min = []byte(tmp[0])
	s.max = []byte(tmp[len(tmp)-1])
}

type int64stats struct {
	min int64
	max int64
}

func newInt64stats() *int64stats {
	return &int64stats{
		min: int64(math.MaxInt64),
	}
}

func (i *int64stats) add(val int64) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *int64stats) bytes(val int64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int64stats) NullCount() *int64 {
	return nil
}

func (f *int64stats) DistinctCount() *int64 {
	return nil
}

func (f *int64stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *int64stats) Max() []byte {
	return f.bytes(f.max)
}

type float64stats struct {
	min float64
	max float64
}

func newFloat64stats() *float64stats {
	return &float64stats{
		min: float64(math.MaxFloat64),
	}
}

func (i *float64stats) add(val float64) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *float64stats) bytes(val float64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *float64stats) NullCount() *int64 {
	return nil
}

func (f *float64stats) DistinctCount() *int64 {
	return nil
}

func (f *float64stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *float64stats) Max() []byte {
	return f.bytes(f.max)
}

type boolStats struct{}

func newBoolStats() *boolStats             { return &boolStats{} }
func (b *boolStats) NullCount() *int64     { return nil }
func (b *boolStats) DistinctCount() *int64 { return nil }
func (b *boolStats) Min() []byte           { return nil }
func (b *boolStats) Max() []byte           { return nil }

func pint32(i int32) *int32       { return &i }
func puint32(i uint32) *uint32    { return &i }
func pint64(i int64) *int64       { return &i }
func puint64(i uint64) *uint64    { return &i }
func pbool(b bool) *bool          { return &b }
func pstring(s string) *string    { return &s }
func pfloat32(f float32) *float32 { return &f }
func pfloat64(f float64) *float64 { return &f }

// keeps track of the indices of repeated fields
// that have already been handled by a previous field
type indices []int

func (i indices) rep(rep uint8) {
	if rep > 0 {
		r := int(rep) - 1
		i[r] = i[r] + 1
		for j := int(rep); j < len(i); j++ {
			i[j] = 0
		}
	}
}

func maxDef(types []int) uint8 {
	var out uint8
	for _, typ := range types {
		if typ > 0 {
			out++
		}
	}
	return out
}

func Int32Type(se *sch.SchemaElement) {
	t := sch.Type_INT32
	se.Type = &t
}

func Uint32Type(se *sch.SchemaElement) {
	t := sch.Type_INT32
	se.Type = &t
	ct := sch.ConvertedType_UINT_32
	se.ConvertedType = &ct
}

func Int64Type(se *sch.SchemaElement) {
	t := sch.Type_INT64
	se.Type = &t
}

func Uint64Type(se *sch.SchemaElement) {
	t := sch.Type_INT64
	se.Type = &t
	ct := sch.ConvertedType_UINT_64
	se.ConvertedType = &ct
}

func Float32Type(se *sch.SchemaElement) {
	t := sch.Type_FLOAT
	se.Type = &t
}

func Float64Type(se *sch.SchemaElement) {
	t := sch.Type_DOUBLE
	se.Type = &t
}

func BoolType(se *sch.SchemaElement) {
	t := sch.Type_BOOLEAN
	se.Type = &t
}

func StringType(se *sch.SchemaElement) {
	t := sch.Type_BYTE_ARRAY
	se.Type = &t
}
