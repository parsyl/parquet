package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/parsyl/parquet/generated"
)

// Field holds the type information for a parquet column
type Field struct {
	Name           string
	Type           FieldFunc
	RepetitionType FieldFunc
}

// Page keeps track of metadata for each ColumnChunk
type Page struct {
	// N is the number of values in the ColumnChunk
	N      int
	Size   int
	Offset int64
	Codec  sch.CompressionCodec
}

type schema struct {
	schema []*sch.SchemaElement
	lookup map[string]sch.SchemaElement
}

// Metadata keeps track of the things that need to
// be kept track of in order to write the FileMetaData
// at the end of the parquet file.
type Metadata struct {
	ts        *thrift.TSerializer
	schema    schema
	rows      int64
	rowGroups []RowGroup

	metadata *sch.FileMetaData
}

// New returns a Metadata struct and reads the first row group
// into memory.
func New(fields ...Field) *Metadata {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	m := &Metadata{
		ts:     ts,
		schema: schemaElements(fields),
	}

	m.StartRowGroup(fields...)
	return m
}

// StartRowGroup is called when starting a new row group
func (m *Metadata) StartRowGroup(fields ...Field) {
	m.rowGroups = append(m.rowGroups, RowGroup{
		fields:  schemaElements(fields),
		columns: make(map[string]sch.ColumnChunk),
	})
}

// RowGroups returns a summary of each schema.RowGroup
func (m *Metadata) RowGroups() []RowGroup {
	rgs := make([]RowGroup, len(m.metadata.RowGroups))
	for i, rg := range m.metadata.RowGroups {
		rgs[i] = RowGroup{
			rowGroup: *rg,
			Rows:     rg.NumRows,
		}
	}
	return rgs
}

// WritePageHeader is called when no more data is written to a column chunk
func (m *Metadata) WritePageHeader(w io.Writer, col string, dataLen, compressedLen, count int, comp sch.CompressionCodec) error {
	m.rows += int64(count)
	ph := &sch.PageHeader{
		Type:                 sch.PageType_DATA_PAGE,
		UncompressedPageSize: int32(dataLen),
		CompressedPageSize:   int32(compressedLen),
		DataPageHeader: &sch.DataPageHeader{
			NumValues:               int32(count),
			Encoding:                sch.Encoding_PLAIN,
			DefinitionLevelEncoding: sch.Encoding_RLE,
			RepetitionLevelEncoding: sch.Encoding_RLE,
		},
	}

	buf, err := m.ts.Write(context.TODO(), ph)
	if err != nil {
		return err
	}

	if err := m.updateRowGroup(col, dataLen, compressedLen, len(buf), count, comp); err != nil {
		return err
	}

	_, err = w.Write(buf)
	return err
}

func (m *Metadata) updateRowGroup(col string, dataLen, compressedLen, headerLen, count int, comp sch.CompressionCodec) error {
	i := len(m.rowGroups)
	if i == 0 {
		return fmt.Errorf("no row groups, you must call StartRowGroup at least once")
	}

	rg := m.rowGroups[i-1]

	rg.rowGroup.NumRows += int64(count)
	err := rg.updateColumnChunk(col, dataLen+headerLen, compressedLen+headerLen, count, m.schema, comp)
	m.rowGroups[i-1] = rg
	return err
}

func columnType(col string, fields schema) (sch.Type, error) {
	for _, f := range fields.schema {
		if f.Name == col {
			return *f.Type, nil
		}
	}

	return 0, fmt.Errorf("could not find type for column %s", col)
}

func (m *Metadata) Rows() int64 {
	return m.metadata.NumRows
}

// Footer writes the FileMetaData at the end of the file.
func (m *Metadata) Footer(w io.Writer) error {
	f := &sch.FileMetaData{
		Version:   1,
		Schema:    m.schema.schema,
		NumRows:   m.rows / int64(len(m.schema.schema)-1),
		RowGroups: make([]*sch.RowGroup, 0, len(m.rowGroups)),
	}

	pos := int64(4)
	for _, mrg := range m.rowGroups {
		rg := mrg.rowGroup
		if rg.NumRows == 0 {
			continue
		}

		for _, col := range mrg.fields.schema[1:] {
			ch, ok := mrg.columns[col.Name]
			if !ok {
				return fmt.Errorf("unknown column %s", col.Name)
			}

			ch.FileOffset = pos
			ch.MetaData.DataPageOffset = pos
			rg.TotalByteSize += ch.MetaData.TotalCompressedSize
			rg.Columns = append(rg.Columns, &ch)
			pos += ch.MetaData.TotalCompressedSize
		}

		rg.NumRows = rg.NumRows / int64(len(mrg.fields.schema)-1)
		f.RowGroups = append(f.RowGroups, &rg)
	}

	buf, err := m.ts.Write(context.TODO(), f)
	if err != nil {
		return err
	}

	n, err := w.Write(buf)
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, uint32(n))
}

// RowGroup wraps schema.RowGroup and adds accounting functions
// that are used to keep track of number of rows written, byte size,
// etc.
type RowGroup struct {
	fields   schema
	rowGroup sch.RowGroup
	columns  map[string]sch.ColumnChunk
	child    *RowGroup

	Rows int64
}

// Columns returns the Columns of the row group.
func (r *RowGroup) Columns() []*sch.ColumnChunk {
	return r.rowGroup.Columns
}

func (r *RowGroup) updateColumnChunk(col string, dataLen, compressedLen, count int, fields schema, comp sch.CompressionCodec) error {
	ch, ok := r.columns[col]
	if !ok {
		t, err := columnType(col, fields)
		if err != nil {
			return err
		}

		ch = sch.ColumnChunk{
			MetaData: &sch.ColumnMetaData{
				Type:         t,
				Encodings:    []sch.Encoding{sch.Encoding_PLAIN},
				PathInSchema: []string{col},
				Codec:        comp,
			},
		}
	}

	ch.MetaData.NumValues += int64(count)
	ch.MetaData.TotalUncompressedSize += int64(dataLen)
	ch.MetaData.TotalCompressedSize += int64(compressedLen)
	r.columns[col] = ch
	return nil
}

func schemaElements(fields []Field) schema {
	out := make([]*sch.SchemaElement, len(fields)+1)
	m := make(map[string]sch.SchemaElement)
	l := int32(len(fields))
	rt := sch.FieldRepetitionType_REQUIRED
	out[0] = &sch.SchemaElement{
		RepetitionType: &rt,
		Name:           "root",
		NumChildren:    &l,
	}

	for i, f := range fields {
		var z int32
		se := &sch.SchemaElement{
			Name:       f.Name,
			TypeLength: &z,
			Scale:      &z,
			Precision:  &z,
			FieldID:    &z,
		}

		f.Type(se)
		f.RepetitionType(se)
		out[i+1] = se
		m[f.Name] = *se
	}

	return schema{schema: out, lookup: m}
}

// Pages maps each column name its Pages
func (m *Metadata) Pages() (map[string][]Page, error) {
	if len(m.metadata.RowGroups) == 0 {
		return nil, nil
	}
	out := map[string][]Page{}
	for _, rg := range m.metadata.RowGroups {
		for _, ch := range rg.Columns {
			pth := ch.MetaData.PathInSchema
			se, ok := m.schema.lookup[pth[len(pth)-1]]
			if !ok {
				return nil, fmt.Errorf("could not find schema for %v", pth)
			}

			pg := Page{
				N:      int(ch.MetaData.NumValues),
				Offset: ch.FileOffset,
				Size:   int(ch.MetaData.TotalCompressedSize),
				Codec:  ch.MetaData.Codec,
			}
			out[se.Name] = append(out[se.Name], pg)
		}
	}
	return out, nil
}

// PageHeader reads the page header from a column page
func (m *Metadata) PageHeader(r io.ReadSeeker) (*sch.PageHeader, error) {
	p := thrift.NewTCompactProtocol(&thrift.StreamTransport{Reader: r})
	pg := &sch.PageHeader{}
	err := pg.Read(p)
	return pg, err
}

// ReadMetaData reads the FileMetaData from the end of a parquet file
func ReadMetaData(r io.ReadSeeker) (*sch.FileMetaData, error) {
	p := thrift.NewTCompactProtocol(&thrift.StreamTransport{Reader: r})
	size, err := getMetaDataSize(r)
	if err != nil {
		return nil, err
	}

	_, err = r.Seek(-int64(size+8), io.SeekEnd)
	if err != nil {
		return nil, err
	}

	m := sch.NewFileMetaData()
	return m, m.Read(p)
}

// ReadFooter reads the parquet metadata
func (m *Metadata) ReadFooter(r io.ReadSeeker) error {
	meta, err := ReadMetaData(r)
	m.metadata = meta
	return err
}

// FieldFunc is used to set some of the metadata for each column
type FieldFunc func(*sch.SchemaElement)

func RepetitionRequired(se *sch.SchemaElement) {
	t := sch.FieldRepetitionType_REQUIRED
	se.RepetitionType = &t
}

func RepetitionOptional(se *sch.SchemaElement) {
	t := sch.FieldRepetitionType_OPTIONAL
	se.RepetitionType = &t
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

func Uint64Type(se *sch.SchemaElement) {
	t := sch.Type_INT64
	se.Type = &t
	ct := sch.ConvertedType_UINT_64
	se.ConvertedType = &ct
}

func Int64Type(se *sch.SchemaElement) {
	t := sch.Type_INT64
	se.Type = &t
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

// GetBools reads a byte array and turns each bit into a bool
func GetBools(r io.Reader, n int, pageSizes []int) ([]bool, error) {
	var vals [8]bool
	data, _ := ioutil.ReadAll(r)
	out := make([]bool, 0, n)
	for _, nVals := range pageSizes {
		if nVals == 0 {
			continue
		}

		l := (nVals / 8)
		if nVals%8 > 0 {
			l++
		}

		var i int
		chunk := data[:l]
		data = data[l:]
		for _, b := range chunk {
			vals = unpackBools(b)
			m := min(nVals, 8)
			for j := 0; j < m; j++ {
				out = append(out, vals[j])
			}
			i += m
			nVals -= m
		}
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func unpackBools(data byte) [8]bool {
	x := uint8(data)
	return [8]bool{
		(x>>0)&1 == 1,
		(x>>1)&1 == 1,
		(x>>2)&1 == 1,
		(x>>3)&1 == 1,
		(x>>4)&1 == 1,
		(x>>5)&1 == 1,
		(x>>6)&1 == 1,
		(x>>7)&1 == 1,
	}
}

func getMetaDataSize(r io.ReadSeeker) (int, error) {
	_, err := r.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	var size uint32
	return int(size), binary.Read(r, binary.LittleEndian, &size)
}
