package doc

// Code generated by github.com/parsyl/parquet.  DO NOT EDIT.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/parsyl/parquet"
	sch "github.com/parsyl/parquet/schema"

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
		NewInt64Field(readDocID, writeDocID, []string{"docid"}, fieldCompression(compression)),
		NewInt64OptionalField(readLinksBackward, writeLinksBackward, []string{"link", "backward"}, []int{1, 2}, optionalFieldCompression(compression)),
		NewInt64OptionalField(readLinksForward, writeLinksForward, []string{"link", "forward"}, []int{1, 2}, optionalFieldCompression(compression)),
		NewStringOptionalField(readNamesLanguagesCode, writeNamesLanguagesCode, []string{"names", "languages", "code"}, []int{2, 2, 0}, optionalFieldCompression(compression)),
		NewStringOptionalField(readNamesLanguagesCountry, writeNamesLanguagesCountry, []string{"names", "languages", "country"}, []int{2, 2, 1}, optionalFieldCompression(compression)),
		NewStringOptionalField(readNamesURL, writeNamesURL, []string{"names", "url"}, []int{2, 1}, optionalFieldCompression(compression)),
	}
}

func readDocID(x Document) int64 {
	return x.DocID
}

func writeDocID(x *Document, vals []int64) {
	x.DocID = vals[0]
}

func readLinksBackward(x Document) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if x.Links == nil {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		if len(x.Links.Backward) == 0 {
			defs = append(defs, 1)
			reps = append(reps, lastRep)
		} else {
			for i0, x0 := range x.Links.Backward {
				if i0 >= 1 {
					lastRep = 1
				}
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, x0)
			}
		}
	}

	return vals, defs, reps
}

func writeLinksBackward(x *Document, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			x.Links = &Link{}
		case 2:
			switch rep {
			case 0:
				x.Links = &Link{Backward: []int64{vals[nVals]}}
			case 1:
				x.Links.Backward = append(x.Links.Backward, vals[nVals])
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readLinksForward(x Document) ([]int64, []uint8, []uint8) {
	var vals []int64
	var defs, reps []uint8
	var lastRep uint8

	if x.Links == nil {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		if len(x.Links.Forward) == 0 {
			defs = append(defs, 1)
			reps = append(reps, lastRep)
		} else {
			for i0, x0 := range x.Links.Forward {
				if i0 >= 1 {
					lastRep = 1
				}
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, x0)
			}
		}
	}

	return vals, defs, reps
}

func writeLinksForward(x *Document, vals []int64, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			x.Links.Forward = append(x.Links.Forward, vals[nVals])
			nVals++
		}
	}

	return nVals, nLevels
}

func readNamesLanguagesCode(x Document) ([]string, []uint8, []uint8) {
	var vals []string
	var defs, reps []uint8
	var lastRep uint8

	if len(x.Names) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			if i0 >= 1 {
				lastRep = 1
			}
			if len(x0.Languages) == 0 {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				for i1, x1 := range x0.Languages {
					if i1 >= 1 {
						lastRep = 2
					}
					defs = append(defs, 2)
					reps = append(reps, lastRep)
					vals = append(vals, x1.Code)
				}
			}
		}
	}

	return vals, defs, reps
}

func writeNamesLanguagesCode(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 2)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 1:
			x.Names = append(x.Names, Name{})
		case 2:
			switch rep {
			case 0, 1:
				x.Names = append(x.Names, Name{Languages: []Language{{Code: vals[nVals]}}})
			case 2:
				x.Names[ind[0]].Languages = append(x.Names[ind[0]].Languages, Language{Code: vals[nVals]})
			}
			nVals++
		}
	}

	return nVals, nLevels
}

func readNamesLanguagesCountry(x Document) ([]string, []uint8, []uint8) {
	var vals []string
	var defs, reps []uint8
	var lastRep uint8

	if len(x.Names) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			if i0 >= 1 {
				lastRep = 1
			}
			if len(x0.Languages) == 0 {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				for i1, x1 := range x0.Languages {
					if i1 >= 1 {
						lastRep = 2
					}
					if x1.Country == nil {
						defs = append(defs, 2)
						reps = append(reps, lastRep)
					} else {
						defs = append(defs, 3)
						reps = append(reps, lastRep)
						vals = append(vals, *x1.Country)
					}
				}
			}
		}
	}

	return vals, defs, reps
}

func writeNamesLanguagesCountry(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 2)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 3:
			x.Names[ind[0]].Languages[ind[1]].Country = pstring(vals[nVals])
			nVals++
		}
	}

	return nVals, nLevels
}

func readNamesURL(x Document) ([]string, []uint8, []uint8) {
	var vals []string
	var defs, reps []uint8
	var lastRep uint8

	if len(x.Names) == 0 {
		defs = append(defs, 0)
		reps = append(reps, lastRep)
	} else {
		for i0, x0 := range x.Names {
			if i0 >= 1 {
				lastRep = 1
			}
			if x0.URL == nil {
				defs = append(defs, 1)
				reps = append(reps, lastRep)
			} else {
				defs = append(defs, 2)
				reps = append(reps, lastRep)
				vals = append(vals, *x0.URL)
			}
		}
	}

	return vals, defs, reps
}

func writeNamesURL(x *Document, vals []string, defs, reps []uint8) (int, int) {
	var nVals, nLevels int
	ind := make(indices, 1)

	for i := range defs {
		def := defs[i]
		rep := reps[i]
		if i > 0 && rep == 0 {
			break
		}

		nLevels++
		ind.rep(rep)

		switch def {
		case 2:
			x.Names[ind[0]].URL = pstring(vals[nVals])
			nVals++
		}
	}

	return nVals, nLevels
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

func (p *ParquetWriter) Add(rec Document) {
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
	Add(r Document)
	Write(w io.Writer, meta *parquet.Metadata) error
	Schema() parquet.Field
	Scan(r *Document)
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

func (p *ParquetReader) Scan(x *Document) {
	if p.err != nil {
		return
	}

	for _, name := range p.fieldNames {
		f := p.fields[name]
		f.Scan(x)
	}
}

type Int64Field struct {
	vals []int64
	parquet.RequiredField
	read  func(r Document) int64
	write func(r *Document, vals []int64)
	stats *int64stats
}

func NewInt64Field(read func(r Document) int64, write func(r *Document, vals []int64), path []string, opts ...func(*parquet.RequiredField)) *Int64Field {
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
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
}

func (f *Int64Field) Scan(r *Document) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *Int64Field) Add(r Document) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}

func (f *Int64Field) Levels() ([]uint8, []uint8) {
	return nil, nil
}

type Int64OptionalField struct {
	parquet.OptionalField
	vals  []int64
	read  func(r Document) ([]int64, []uint8, []uint8)
	write func(r *Document, vals []int64, def, rep []uint8) (int, int)
	stats *int64optionalStats
}

func NewInt64OptionalField(read func(r Document) ([]int64, []uint8, []uint8), write func(r *Document, vals []int64, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *Int64OptionalField {
	return &Int64OptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newint64optionalStats(maxDef(types)),
	}
}

func (f *Int64OptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: Int64Type, RepetitionType: f.RepetitionType, Types: f.Types}
}

func (f *Int64OptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.Defs), f.stats)
}

func (f *Int64OptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v := make([]int64, f.Values()-len(f.vals))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *Int64OptionalField) Add(r Document) {
	vals, defs, reps := f.read(r)
	f.stats.add(vals, defs)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *Int64OptionalField) Scan(r *Document) {
	if len(f.Defs) == 0 {
		return
	}

	v, l := f.write(r, f.vals, f.Defs, f.Reps)
	f.vals = f.vals[v:]
	f.Defs = f.Defs[l:]
	if len(f.Reps) > 0 {
		f.Reps = f.Reps[l:]
	}
}

func (f *Int64OptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}

type StringOptionalField struct {
	parquet.OptionalField
	vals  []string
	read  func(r Document) ([]string, []uint8, []uint8)
	write func(r *Document, vals []string, def, rep []uint8) (int, int)
	stats *stringOptionalStats
}

func NewStringOptionalField(read func(r Document) ([]string, []uint8, []uint8), write func(r *Document, vals []string, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *StringOptionalField {
	return &StringOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newStringOptionalStats(maxDef(types)),
	}
}

func (f *StringOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: StringType, RepetitionType: f.RepetitionType, Types: f.Types}
}

func (f *StringOptionalField) Add(r Document) {
	vals, defs, reps := f.read(r)
	f.stats.add(vals, defs)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *StringOptionalField) Scan(r *Document) {
	if len(f.Defs) == 0 {
		return
	}

	v, l := f.write(r, f.vals, f.Defs, f.Reps)
	f.vals = f.vals[v:]
	f.Defs = f.Defs[l:]
	if len(f.Reps) > 0 {
		f.Reps = f.Reps[l:]
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

	return f.DoWrite(w, meta, buf.Bytes(), len(f.Defs), f.stats)
}

func (f *StringOptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	for j := 0; j < f.Values(); j++ {
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

func (f *StringOptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
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

type int64optionalStats struct {
	min     int64
	max     int64
	nils    int64
	nonNils int64
	maxDef  uint8
}

func newint64optionalStats(d uint8) *int64optionalStats {
	return &int64optionalStats{
		min:    int64(math.MaxInt64),
		maxDef: d,
	}
}

func (f *int64optionalStats) add(vals []int64, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < f.maxDef {
			f.nils++
		} else {
			val := vals[i]
			i++

			f.nonNils++
			if val < f.min {
				f.min = val
			}
			if val > f.max {
				f.max = val
			}
		}
	}
}

func (f *int64optionalStats) bytes(val int64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int64optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *int64optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *int64optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *int64optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}

type stringOptionalStats struct {
	vals   []string
	min    []byte
	max    []byte
	nils   int64
	maxDef uint8
}

func newStringOptionalStats(d uint8) *stringOptionalStats {
	return &stringOptionalStats{maxDef: d}
}

func (s *stringOptionalStats) add(vals []string, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < s.maxDef {
			s.nils++
		} else {
			s.vals = append(s.vals, vals[i])
			i++
		}
	}
}

func (s *stringOptionalStats) NullCount() *int64 {
	return &s.nils
}

func (s *stringOptionalStats) DistinctCount() *int64 {
	return nil
}

func (s *stringOptionalStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringOptionalStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringOptionalStats) minMax() {
	if len(s.vals) == 0 {
		return
	}

	tmp := make([]string, len(s.vals))
	copy(tmp, s.vals)
	sort.Strings(tmp)
	s.min = []byte(tmp[0])
	s.max = []byte(tmp[len(tmp)-1])
}

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
