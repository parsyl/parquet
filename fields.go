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

type requiredNumField struct {
	vals []interface{}
	col  string
}

func (i *requiredNumField) write(w io.Writer, meta *schema.Metadata, pos int) error {
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

type optionalNumField struct {
	vals []interface{}
	defs []int64
	col  string
}

func (i *optionalNumField) write(w io.Writer, meta *schema.Metadata, pos int) error {
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
	requiredNumField
	val func(r Record) int32
}

func newInt32Field(val func(r Record) int32, col string) *int32Field {
	return &int32Field{
		val:              val,
		requiredNumField: requiredNumField{col: col},
	}
}

func (i *int32Field) add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type int32OptionalField struct {
	optionalNumField
	val func(r Record) *int32
}

func newInt32OptionalField(val func(r Record) *int32, col string) *int32OptionalField {
	return &int32OptionalField{
		val:              val,
		optionalNumField: optionalNumField{col: col},
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
	requiredNumField
	val func(r Record) int64
}

func newInt64Field(val func(r Record) int64, col string) *int64Field {
	return &int64Field{
		val:              val,
		requiredNumField: requiredNumField{col: col},
	}
}

func (i *int64Field) add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type int64OptionalField struct {
	optionalNumField
	val func(r Record) *int64
}

func newInt64OptionalField(val func(r Record) *int64, col string) *int64OptionalField {
	return &int64OptionalField{
		val:              val,
		optionalNumField: optionalNumField{col: col},
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

type float32Field struct {
	requiredNumField
	val func(r Record) float32
}

func newFloat32Field(val func(r Record) float32, col string) *float32Field {
	return &float32Field{
		val:              val,
		requiredNumField: requiredNumField{col: col},
	}
}

func (i *float32Field) add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type float32OptionalField struct {
	optionalNumField
	val func(r Record) *float32
}

func newFloat32OptionalField(val func(r Record) *float32, col string) *float32OptionalField {
	return &float32OptionalField{
		val:              val,
		optionalNumField: optionalNumField{col: col},
	}
}

func (i *float32OptionalField) add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type optionalBoolField struct {
	vals []bool
	defs []int64
	col  string
	val  func(r Record) *bool
}

func newOptionalBoolField(val func(r Record) *bool, col string) *optionalBoolField {
	return &optionalBoolField{
		val: val,
		col: col,
	}
}

func (f *optionalBoolField) add(r Record) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.defs = append(f.defs, 1)
	} else {
		f.defs = append(f.defs, 0)
	}
}

func (f *optionalBoolField) write(w io.Writer, meta *schema.Metadata, pos int) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}

	err := writeLevels(wc, f.defs)
	if err != nil {
		return err
	}

	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	wc.Write(rawBuf)

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, f.col, pos, wc.n, len(compressed), len(f.defs)); err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(compressed))
	return err
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
