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

type requiredIntField struct {
	vals []interface{}
	col  string
}

func (i *requiredIntField) write(w io.Writer, meta *schema.Metadata, pos int) error {
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

type optionalIntField struct {
	vals []interface{}
	defs []int64
	col  string
}

func (i *optionalIntField) write(w io.Writer, meta *schema.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}

	err := writeLevels(wc, i.defs)
	if err != nil {
		return err
	}

	for _, i := range i.vals {
		if err := binary.Write(wc, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, i.col, pos, wc.n, len(compressed), len(i.defs)); err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(compressed))
	return err
}

type int32Field struct {
	requiredIntField
	val func(r Record) int32
}

func newInt32Field(val func(r Record) int32, col string) *int32Field {
	return &int32Field{
		val:              val,
		requiredIntField: requiredIntField{col: col},
	}
}

func (i *int32Field) add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type int32OptionalField struct {
	optionalIntField
	val func(r Record) *int32
}

func newInt32OptionalField(val func(r Record) *int32, col string) *int32OptionalField {
	return &int32OptionalField{
		val:              val,
		optionalIntField: optionalIntField{col: col},
	}
}

func (i *int32OptionalField) add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type int64Field struct {
	requiredIntField
	val func(r Record) int64
}

func newInt64Field(val func(r Record) int64, col string) *int64Field {
	return &int64Field{
		val:              val,
		requiredIntField: requiredIntField{col: col},
	}
}

func (i *int64Field) add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type int64OptionalField struct {
	optionalIntField
	val func(r Record) *int64
}

func newInt64OptionalField(val func(r Record) *int64, col string) *int64OptionalField {
	return &int64OptionalField{
		val:              val,
		optionalIntField: optionalIntField{col: col},
	}
}

func (i *int64OptionalField) add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type requiredStringField struct {
	vals []string
	col  string
	val  func(r Record) string
}

func newStringField(val func(r Record) string, col string) *requiredStringField {
	return &requiredStringField{
		val: val,
		col: col,
	}
}

func (f *requiredStringField) add(r Record) {
	f.vals = append(f.vals, f.val(r))
}

func (f *requiredStringField) write(w io.Writer, meta *schema.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}

	for _, s := range f.vals {
		if err := binary.Write(wc, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		wc.Write([]byte(s))
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, f.col, pos, wc.n, len(compressed), len(f.vals)); err != nil {
		return err
	}

	_, err := io.Copy(w, bytes.NewBuffer(compressed))
	return err
}