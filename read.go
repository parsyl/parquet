package parquet

import (
	"encoding/binary"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/parsyl/parquet/generated"
)

func (m *Metadata) Read(r io.ReadSeeker) error {
	pf := thrift.NewTCompactProtocolFactory()
	m.protocol = pf.GetProtocol(thrift.NewStreamTransportR(r))
	if err := m.readFooter(r); err != nil {
		return err
	}

	return nil
}

type Position struct {
	N      int
	Size   int
	Offset int
}

func (m *Metadata) Rows() int {
	return int(m.metadata.NumRows)
}

func (m *Metadata) Offsets() map[string][]Position {
	if len(m.metadata.RowGroups) == 0 {
		return nil
	}

	out := map[string][]Position{}
	rg := m.metadata.RowGroups[0]
	for i, ch := range rg.Columns {
		se := m.schema[i]
		pos := Position{
			N:      int(ch.MetaData.NumValues),
			Offset: int(ch.FileOffset),
			Size:   int(ch.MetaData.TotalCompressedSize),
		}
		out[se.Name] = append(out[se.Name], pos)
	}
	return out
}

func (m *Metadata) readFooter(r io.ReadSeeker) error {
	size, err := m.getSize(r)
	if err != nil {
		return err
	}

	_, err = r.Seek(-int64(size+8), io.SeekEnd)

	m.metadata = sch.NewFileMetaData()
	if err := m.metadata.Read(m.protocol); err != nil {
		return err
	}

	return nil
}

func (m *Metadata) getSize(r io.ReadSeeker) (int, error) {
	_, err := r.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	var size uint32
	return int(size), binary.Read(r, binary.LittleEndian, &size)
}
