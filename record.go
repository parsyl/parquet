package parquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cswank/parquet/schema"
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

	// records are for subsequent chunks
	records *Records
	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta *schema.Metadata
	w    *writeCounter
}

func New(w io.Writer) *Records {
	return &Records{
		max: 1000,
		w:   &writeCounter{w: w},
		meta: schema.New(
			schema.Field{Name: "id", Type: schema.Int32Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "age", Type: schema.Int32Type, RepetitionType: schema.RepetitionOptional},
		),
	}
}

func (r *Records) Write() error {
	if _, err := r.w.Write([]byte("PAR1")); err != nil {
		return err
	}

	if err := r.writeID(); err != nil {
		return err
	}

	for child := r.records; child != nil; child = child.records {
		child := r.records
		if child == nil {
			break
		}

		if err := child.writeID(); err != nil {
			return err
		}
	}

	if err := r.writeAge(); err != nil {
		return err
	}

	for child := r.records; child != nil; child = child.records {
		if err := child.writeAge(); err != nil {
			return err
		}
	}

	_, err := r.w.Write([]byte("PAR1"))
	return err
}

func (r *Records) writeID() error {
	pos := r.w.n
	buf := bytes.Buffer{}
	w := &writeCounter{w: &buf}

	for _, i := range r.ID {
		if err := binary.Write(w, binary.BigEndian, i); err != nil {
			return err
		}
	}

	size := w.n - pos
	compressed := snappy.Encode(nil, buf.Bytes())

	if err := r.meta.WritePageHeader(r.w, "id", pos, size, len(compressed), len(r.ID)); err != nil {
		return err
	}

	_, err := io.Copy(r.w, bytes.NewBuffer(compressed))
	return err
}

func (r *Records) writeAge() error {
	pos := r.w.n
	buf := bytes.Buffer{}
	w := &writeCounter{w: &buf}

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

	size := w.n - pos
	compressed := snappy.Encode(nil, buf.Bytes())
	if err := r.meta.WritePageHeader(r.w, "age", pos, size, len(compressed), len(r.AgeDefs)); err != nil {
		return err
	}

	_, err := io.Copy(r.w, bytes.NewBuffer(compressed))
	return err
}

func (r *Records) Add(rec Record) {
	if len(r.ID) == r.max {
		if r.records == nil {
			r.records = New(r.w)
		}
		r.records.Add(rec)
		return
	}

	r.ID = append(r.ID, rec.ID)
	if rec.Age != nil {
		r.Age = append(r.Age, *rec.Age)
		r.AgeDefs = append(r.IDDefs, 1)
	} else {
		r.AgeDefs = append(r.AgeDefs, 0)
	}
}

type Record struct {
	ID  int32  `parquet:"name=id, type=INT32"`
	Age *int32 `parquet:"name=age, type=INT32"`
}

type writeCounter struct {
	n int
	w io.Writer
}

func (w *writeCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += n
	return n, err
}
