package schema

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/cswank/parquet/schema/generated"
)

type Field struct {
	Name           string
	Type           FieldFunc
	RepetitionType FieldFunc
}

type Metadata struct {
	ts        *thrift.TSerializer
	fields    []*sch.SchemaElement
	rows      int64
	size      int64
	rowGroups []rowGroup
}

func New(fields ...Field) *Metadata {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	m := &Metadata{
		ts:     ts,
		fields: schemaElements(fields),
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
func (m *Metadata) WritePageHeader(w io.Writer, col string, pos, dataLen, compressedLen, count int) error {
	m.size += int64(dataLen)
	m.rows += int64(count)

	ph := &sch.PageHeader{
		UncompressedPageSize: int32(dataLen),
		CompressedPageSize:   int32(compressedLen),
		DataPageHeader: &sch.DataPageHeader{
			NumValues:               int32(count),
			DefinitionLevelEncoding: sch.Encoding_PLAIN,
			RepetitionLevelEncoding: sch.Encoding_PLAIN,
		},
	}

	buf, err := m.ts.Write(context.TODO(), ph)
	if err != nil {
		return err
	}

	m.updateRowGroup(col, pos, dataLen, compressedLen, len(buf), count)
	_, err = io.Copy(w, bytes.NewBuffer(buf))
	return err
}

func (m *Metadata) updateRowGroup(col string, pos, dataLen, compressedLen, headerLen, count int) error {
	i := len(m.rowGroups)
	if i == 0 {
		return fmt.Errorf("no row groups, you must call StartRowGroup at least once")
	}

	rg := m.rowGroups[i-1]
	rg.rowGroup.NumRows += int64(count)
	rg.rowGroup.TotalByteSize += int64(dataLen + headerLen)
	err := rg.updateColumnChunk(col, pos, dataLen, compressedLen, count, m.fields)
	m.rowGroups[i-1] = rg
	return err
}

func (r *rowGroup) columnType(col string, fields []*sch.SchemaElement) (sch.Type, error) {
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
			ch, ok := rg.columns[col.Name]
			if !ok {
				return fmt.Errorf("unknown column %s", col.Name)
			}
			rg.rowGroup.TotalByteSize += ch.MetaData.TotalUncompressedSize
			rg.rowGroup.Columns = append(rg.rowGroup.Columns, &ch)
		}

		rgs[i] = &rg.rowGroup
	}

	f := &sch.FileMetaData{
		Version:   1,
		Schema:    m.fields,
		NumRows:   m.rows,
		RowGroups: rgs,
	}

	buf, err := m.ts.Write(context.TODO(), f)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(buf))
	return err
}

type rowGroup struct {
	fields   []*sch.SchemaElement
	rowGroup sch.RowGroup
	columns  map[string]sch.ColumnChunk
}

func (r *rowGroup) updateColumnChunk(col string, pos, dataLen, compressedLen, count int, fields []*sch.SchemaElement) error {
	ch, ok := r.columns[col]
	if !ok {
		t, err := r.columnType(col, fields)
		if err != nil {
			return err
		}

		ch = sch.ColumnChunk{
			MetaData: &sch.ColumnMetaData{
				Type:           t,
				Encodings:      []sch.Encoding{sch.Encoding_PLAIN},
				PathInSchema:   nil, //TODO lookup
				DataPageOffset: int64(pos),
				Codec:          sch.CompressionCodec_SNAPPY,
			},
		}
	}

	ch.MetaData.NumValues += int64(count)
	ch.MetaData.TotalUncompressedSize += int64(compressedLen)
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
		Name:           "parquet_go_root",
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
	i := sch.Type_INT32
	se.Type = &i
}
