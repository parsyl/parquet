package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/parsyl/parquet/generated"
)

type Field struct {
	Name           string
	Type           FieldFunc
	RepetitionType FieldFunc
}

type Position struct {
	N      int
	Size   int
	Offset int64
}

type schema struct {
	schema []*sch.SchemaElement
	lookup map[string]sch.SchemaElement
}

type Metadata struct {
	ts        *thrift.TSerializer
	schema    schema
	rows      int64
	rowGroups []rowGroup

	metadata *sch.FileMetaData
}

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

func (m *Metadata) Footer(w io.Writer) error {

	f := &sch.FileMetaData{
		Version:   1,
		Schema:    m.schema.schema,
		NumRows:   m.rows / int64(len(m.schema.schema)-1),
		RowGroups: make([]*sch.RowGroup, 0, len(m.rowGroups)),
	}

	for _, mrg := range m.rowGroups {
		rg := mrg.rowGroup
		if rg.NumRows == 0 {
			continue
		}

		for _, col := range mrg.fields.schema {
			if col.Name == "root" {
				continue
			}

			ch, ok := mrg.columns[col.Name]
			if !ok {
				return fmt.Errorf("unknown column %s", col.Name)
			}

			rg.TotalByteSize += ch.MetaData.TotalCompressedSize
			rg.Columns = append(rg.Columns, &ch)
		}

		rg.NumRows = rg.NumRows / int64(len(mrg.fields.schema)-1)
		f.RowGroups = append(f.RowGroups, &rg)
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
	fields   schema
	rowGroup sch.RowGroup
	columns  map[string]sch.ColumnChunk
	child    *rowGroup
}

func (r *rowGroup) updateColumnChunk(col string, pos int64, dataLen, compressedLen, count int, fields schema) error {
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

func (m *Metadata) Offsets() (map[string][]Position, error) {
	if len(m.metadata.RowGroups) == 0 {
		return nil, nil
	}
	out := map[string][]Position{}
	for _, rg := range m.metadata.RowGroups {
		for _, ch := range rg.Columns {
			pth := ch.MetaData.PathInSchema
			se, ok := m.schema.lookup[pth[len(pth)-1]]
			if !ok {
				return nil, fmt.Errorf("could not find schema for %v", pth)
			}

			pos := Position{
				N:      int(ch.MetaData.NumValues),
				Offset: ch.FileOffset,
				Size:   int(ch.MetaData.TotalCompressedSize),
			}
			out[se.Name] = append(out[se.Name], pos)
		}
	}
	return out, nil
}

func (m *Metadata) PageHeader(r io.ReadSeeker) (*sch.PageHeader, error) {
	p := thrift.NewTCompactProtocol(&thrift.StreamTransport{Reader: r})
	pg := &sch.PageHeader{}
	err := pg.Read(p)
	return pg, err
}

func (m *Metadata) getSize(r io.ReadSeeker) (int, error) {
	_, err := r.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	var size uint32
	return int(size), binary.Read(r, binary.LittleEndian, &size)
}

func (m *Metadata) ReadFooter(r io.ReadSeeker) error {
	p := thrift.NewTCompactProtocol(&thrift.StreamTransport{Reader: r})
	size, err := m.getSize(r)
	if err != nil {
		return err
	}

	_, err = r.Seek(-int64(size+8), io.SeekEnd)
	if err != nil {
		return err
	}

	m.metadata = sch.NewFileMetaData()
	return m.metadata.Read(p)
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

func GetBools(r io.Reader, n int) ([]bool, error) {
	var index int
	var vals [8]uint32
	data, _ := ioutil.ReadAll(r)
	out := make([]bool, n)

	for i := 0; i < n; i++ {
		if index == 0 {
			if len(data) == 0 {
				return nil, errors.New("not enough data to decode all values")
			}
			vals = unpack8uint32(data[:1])
			data = data[1:]
		}
		out[i] = vals[index] == 1
		index = (index + 1) % 8
	}
	return out, nil
}

func unpack8uint32(data []byte) [8]uint32 {
	var a [8]uint32
	a[0] = uint32((data[0]>>0)&1) << 0
	a[1] = uint32((data[0]>>1)&1) << 0
	a[2] = uint32((data[0]>>2)&1) << 0
	a[3] = uint32((data[0]>>3)&1) << 0
	a[4] = uint32((data[0]>>4)&1) << 0
	a[5] = uint32((data[0]>>5)&1) << 0
	a[6] = uint32((data[0]>>6)&1) << 0
	a[7] = uint32((data[0]>>7)&1) << 0
	return a
}

// WriteLevels writes vals to w as RLE encoded data
func WriteLevels(w io.Writer, vals []int64) error {
	var max uint64
	if len(vals) > 0 {
		max = 1
	}

	rleBuf := writeRLE(vals, int32(bitNum(max)))
	res := make([]byte, 0)
	binary.Write(w, binary.LittleEndian, int32(len(rleBuf)))
	res = append(res, rleBuf...)
	_, err := w.Write(res)
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
	var bitn uint64
	for ; num != 0; num >>= 1 {
		bitn++
	}
	return bitn
}

// ReadLevels reads the RLE encoded definition levels
func ReadLevels(r io.Reader) ([]int64, int, error) {
	bitWidth := bitNum(1) //TODO: figure out if this is correct

	res := make([]int64, 0)
	var l int32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return res, 0, err
	}

	buf := make([]byte, l)
	if _, err := r.Read(buf); err != nil {
		return res, 0, err
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header, err := readUnsignedVarInt(newReader)
		if err != nil {
			return res, 0, err
		}
		if header&1 == 0 {
			buf, err := readRLE(newReader, header, bitWidth)
			if err != nil {
				return res, 0, err
			}
			res = append(res, buf...)

		} else {
			buf, err := readBitPacked(newReader, header, bitWidth)
			if err != nil {
				return res, 0, err
			}
			res = append(res, buf...)
		}
	}
	return res, int(l + 4), nil
}

func readBitPacked(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	var err error
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8

	res := make([]int64, 0, cnt)

	if cnt == 0 {
		return res, nil
	}

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, int64(0))
		}
		return res, err
	}
	bytesBuf := make([]byte, byteCnt)
	if _, err = r.Read(bytesBuf); err != nil {
		return res, err
	}

	i := 0
	var resCur uint64 = 0
	var resCurNeedBits uint64 = bitWidth
	var used uint64 = 0
	var left uint64 = 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= uint64(((uint64(b) >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(bitWidth-resCurNeedBits))
			res = append(res, int64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i += 1
				b = bytesBuf[i]
				left = 8
				used = 0
			}

		} else {
			resCur |= uint64((uint64(b) >> uint64(used)) << uint64(bitWidth-resCurNeedBits))
			i += 1
			if i < len(bytesBuf) {
				b = bytesBuf[i]
			}
			resCurNeedBits -= left
			left = 8
			used = 0
		}
	}
	return res, err
}

func readRLE(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	var err error
	var res []int64
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = r.Read(data); err != nil {
			return res, err
		}
	}
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := int64(binary.LittleEndian.Uint32(data))
	res = make([]int64, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res, err
}

func readUnsignedVarInt(r io.Reader) (uint64, error) {
	var err error
	var res uint64 = 0
	var shift uint64 = 0
	for {
		b := make([]byte, 1)
		_, err := r.Read(b)
		if err != nil {
			break
		}
		res |= ((uint64(b[0]) & uint64(0x7F)) << uint64(shift))
		if (b[0] & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return res, err
}
