package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/apache/thrift/lib/go/thrift"
	sch "github.com/parsyl/parquet/generated"
)

func (m *Metadata) ReadFooter(r io.ReadSeeker) error {
	pf := thrift.NewTCompactProtocolFactory()
	p := pf.GetProtocol(thrift.NewStreamTransportR(r))
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

type Position struct {
	N      int
	Size   int
	Offset int64
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
	ttransport := &thrift.StreamTransport{Reader: r}
	p := thrift.NewTCompactProtocol(ttransport)
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
