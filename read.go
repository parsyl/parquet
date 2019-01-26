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

func (m *Metadata) Rows() int {
	return int(m.metadata.NumRows)
}

func (m *Metadata) ReadChunks(i int) error {

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
