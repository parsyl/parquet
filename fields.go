package parquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cswank/parquet/schema"
	"github.com/golang/snappy"
)

type Field interface {
	add(r Record)
	write(w io.Writer, meta *schema.Metadata, pos int) error
}

type Int32Field struct {
	vals []int32
	val  func(r Record) int32
	col  string
}

func (i *Int32Field) add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

func (i *Int32Field) write(w io.Writer, meta *schema.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}

	for _, i := range i.vals {
		if err := binary.Write(wc, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, i.col, pos, wc.n, len(compressed), len(i.vals)); err != nil {
		return err
	}

	_, err := io.Copy(w, bytes.NewBuffer(compressed))
	return err
}

type Int32OptionalField struct {
	vals []int32
	defs []int64
	val  func(r Record) *int32
	col  string
}

func (i *Int32OptionalField) add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

func (i *Int32OptionalField) write(w io.Writer, meta *schema.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}

	err := writeLevels(w, i.defs)
	if err != nil {
		return err
	}

	for _, i := range i.vals {
		if err := binary.Write(wc, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, i.col, pos, wc.n, len(compressed), len(i.vals)); err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(compressed))
	return err
}
