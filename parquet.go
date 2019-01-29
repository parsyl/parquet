package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/parsyl/parquet/generated"
)

type Field struct {
	Name           string
	Type           FieldFunc
	RepetitionType FieldFunc
}

type Metadata struct {
	ts        *thrift.TSerializer
	schema    []*sch.SchemaElement
	rows      int64
	rowGroups []rowGroup

	//for reading
	protocol thrift.TProtocol
	metadata *sch.FileMetaData
}

func New(fields ...Field) *Metadata {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	m := &Metadata{
		ts:     ts,
		schema: schemaElements(fields),
	}

	// this is due to my not being sure about the purpose of RowGroup in parquet
	m.StartRowGroup(fields...)
	return m
}

func (m *Metadata) StartRowGroup(fields ...Field) {
	m.rowGroups = append(m.rowGroups, rowGroup{
		fields:  schemaElements(fields),
		columns: make(map[string]sch.ColumnChunk),
	})
}

// WritePageHeader indicates you are done writing this columns's chunk
func (m *Metadata) WritePageHeader(w io.Writer, col string, pos int64, dataLen, compressedLen, count int) error {
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

	m.updateRowGroup(col, pos, dataLen, compressedLen, len(buf), count)
	_, err = w.Write(buf)
	return err
}

func (m *Metadata) updateRowGroup(col string, pos int64, dataLen, compressedLen, headerLen, count int) error {
	i := len(m.rowGroups)
	if i == 0 {
		return fmt.Errorf("no row groups, you must call StartRowGroup at least once")
	}

	rg := m.rowGroups[i-1]

	rg.rowGroup.NumRows += int64(count)
	err := rg.updateColumnChunk(col, pos, dataLen+headerLen, compressedLen+headerLen, count, m.schema)
	m.rowGroups[i-1] = rg
	return err
}

func columnType(col string, fields []*sch.SchemaElement) (sch.Type, error) {
	for _, f := range fields {
		if f.Name == col {
			return *f.Type, nil
		}
	}

	return 0, fmt.Errorf("could not find type for column %s", col)
}

func (m *Metadata) Footer(w io.Writer) error {
	rgs := make([]*sch.RowGroup, len(m.rowGroups))
	for i, rg := range m.rowGroups {
		for _, col := range rg.fields {
			if col.Name == "root" {
				continue
			}

			ch, ok := rg.columns[col.Name]
			if !ok {
				return fmt.Errorf("unknown column %s", col.Name)
			}

			rg.rowGroup.TotalByteSize += ch.MetaData.TotalCompressedSize
			rg.rowGroup.Columns = append(rg.rowGroup.Columns, &ch)
		}

		rg.rowGroup.NumRows = rg.rowGroup.NumRows / int64(len(rg.fields)-1)
		rgs[i] = &rg.rowGroup
	}

	f := &sch.FileMetaData{
		Version:   1,
		Schema:    m.schema,
		NumRows:   m.rows / int64(len(m.schema)-1),
		RowGroups: rgs,
	}

	buf, err := m.ts.Write(context.TODO(), f)
	if err != nil {
		return err
	}

	n, err := io.Copy(w, bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, uint32(n))
}

type rowGroup struct {
	fields   []*sch.SchemaElement
	rowGroup sch.RowGroup
	columns  map[string]sch.ColumnChunk
	child    *rowGroup
}

func (r *rowGroup) updateColumnChunk(col string, pos int64, dataLen, compressedLen, count int, fields []*sch.SchemaElement) error {
	ch, ok := r.columns[col]
	if !ok {
		t, err := columnType(col, fields)
		if err != nil {
			return err
		}

		ch = sch.ColumnChunk{
			FileOffset: pos,
			MetaData: &sch.ColumnMetaData{
				Type:           t,
				Encodings:      []sch.Encoding{sch.Encoding_PLAIN},
				PathInSchema:   []string{col},
				DataPageOffset: pos,
				Codec:          sch.CompressionCodec_SNAPPY,
			},
		}
	}

	ch.MetaData.NumValues += int64(count)
	ch.MetaData.TotalUncompressedSize += int64(dataLen)
	ch.MetaData.TotalCompressedSize += int64(compressedLen)
	r.columns[col] = ch
	return nil
}

func schemaElements(fields []Field) []*sch.SchemaElement {
	out := make([]*sch.SchemaElement, len(fields)+1)
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
	}

	return out
}

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
