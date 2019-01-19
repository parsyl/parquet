package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	xit "github.com/xitongsys/parquet-go/parquet"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/golang/snappy"
)

// Records reprents a row group
type Records struct {
	ID      []int32
	IDDefs  []int
	IDReps  []int
	Age     []int32
	AgeDefs []int
	AgeReps []int
	// Records are for subsequent row groups
	records []Records

	w *nWriter
}

func New(w io.Writer) *Records {
	return &Records{
		w: &nWriter{w: w},
	}
}

func (r *Records) Write() error {
	if _, err := r.w.Write([]byte("PAR1")); err != nil {
		return err
	}

	if err := r.writeID(); err != nil {
		return err
	}
	if err := r.writeAge(); err != nil {
		return err
	}

	for _, child := range r.records {
		if err := child.writeID(); err != nil {
			return err
		}
		if err := child.writeAge(); err != nil {
			return err
		}
	}

	_, err := r.w.Write([]byte("PAR1"))
	return err
}

func (r *Records) writeID() error {
	buf := bytes.Buffer{}
	w := &nWriter{w: &buf}

	for _, a := range r.IDReps {
		if err := binary.Write(w, binary.LittleEndian, byte(a)); err != nil {
			return err
		}
	}

	for _, a := range r.IDDefs {
		if err := binary.Write(w, binary.LittleEndian, byte(a)); err != nil {
			return err
		}
	}

	for _, i := range r.ID {
		if err := binary.Write(w, binary.BigEndian, i); err != nil {
			return err
		}
	}
	n := w.n
	compressed := snappy.Encode(nil, buf.Bytes())
	for _, b := range compressed {
		fmt.Printf("%02x\n", b)
	}
	cn := len(compressed)

	if err := r.writePageHeader(n, cn, len(r.ID)); err != nil {
		return err
	}

	_, err := io.Copy(r.w, bytes.NewBuffer(compressed))
	return err
}

func (r *Records) writePageHeader(n, cn, l int) error {
	ph := &xit.PageHeader{
		UncompressedPageSize: int32(n),
		CompressedPageSize:   int32(cn),
		DataPageHeader: &xit.DataPageHeader{
			NumValues:               int32(l),
			DefinitionLevelEncoding: xit.Encoding_PLAIN,
			RepetitionLevelEncoding: xit.Encoding_PLAIN,
		},
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), ph)
	_, err := io.Copy(r.w, bytes.NewBuffer(pageHeaderBuf))
	return err
}

func (r *Records) writeAge() error {
	buf := bytes.Buffer{}
	w := &nWriter{w: &buf}

	for _, a := range r.AgeReps {
		if err := binary.Write(w, binary.LittleEndian, byte(a)); err != nil {
			return err
		}
	}

	for _, a := range r.AgeDefs {
		if err := binary.Write(w, binary.LittleEndian, byte(a)); err != nil {
			return err
		}
	}

	for _, a := range r.Age {
		if err := binary.Write(w, binary.LittleEndian, a); err != nil {
			return err
		}
	}
	n := w.n
	compressed := snappy.Encode(nil, buf.Bytes())
	cn := len(compressed)

	if err := r.writePageHeader(n, cn, len(r.AgeDefs)); err != nil {
		return err
	}

	_, err := io.Copy(r.w, bytes.NewBuffer(compressed))
	return err
}

func (r *Records) Add(rec Record) {
	r.ID = append(r.ID, rec.ID)
	r.IDDefs = append(r.IDDefs, 1)
	r.IDReps = append(r.IDReps, 0)
	if rec.Age != nil {
		r.Age = append(r.Age, *rec.Age)
		r.AgeDefs = append(r.AgeDefs, 1)
		r.AgeReps = append(r.AgeReps, 0)
	} else {
		r.AgeDefs = append(r.AgeDefs, 0)
		r.AgeReps = append(r.AgeReps, 0)
	}
}

type Record struct {
	ID  int32  `parquet:"name=id, type=INT32"`
	Age *int32 `parquet:"name=age, type=INT32"`
}

type nWriter struct {
	n int
	w io.Writer
}

func (w *nWriter) Write(p []byte) (int, error) {
	fmt.Printf("%x\n", p)
	n, err := w.w.Write(p)
	w.n += n
	return n, err
}
