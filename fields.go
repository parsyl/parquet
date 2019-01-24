package parquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cswank/parquet/schema"
	"github.com/golang/snappy"
)

type Field interface {
	Add(r Record)
	Write(w io.Writer, meta *schema.Metadata, pos int) error
}

type RequiredNumField struct {
	vals []interface{}
	col  string
}

func (i *RequiredNumField) Write(w io.Writer, meta *schema.Metadata, pos int) error {
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

type OptionalNumField struct {
	vals []interface{}
	defs []int64
	col  string
}

func (i *OptionalNumField) Write(w io.Writer, meta *schema.Metadata, pos int) error {
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

type Int32Field struct {
	RequiredNumField
	val func(r Record) int32
}

func NewInt32Field(val func(r Record) int32, col string) *Int32Field {
	return &Int32Field{
		val:              val,
		RequiredNumField: RequiredNumField{col: col},
	}
}

func (i *Int32Field) Add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type Int32OptionalField struct {
	OptionalNumField
	val func(r Record) *int32
}

func NewInt32OptionalField(val func(r Record) *int32, col string) *Int32OptionalField {
	return &Int32OptionalField{
		val:              val,
		OptionalNumField: OptionalNumField{col: col},
	}
}

func (i *Int32OptionalField) Add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type Int64Field struct {
	RequiredNumField
	val func(r Record) int64
}

func NewInt64Field(val func(r Record) int64, col string) *Int64Field {
	return &Int64Field{
		val:              val,
		RequiredNumField: RequiredNumField{col: col},
	}
}

func (i *Int64Field) Add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type Int64OptionalField struct {
	OptionalNumField
	val func(r Record) *int64
}

func NewInt64OptionalField(val func(r Record) *int64, col string) *Int64OptionalField {
	return &Int64OptionalField{
		val:              val,
		OptionalNumField: OptionalNumField{col: col},
	}
}

func (i *Int64OptionalField) Add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type Float32Field struct {
	RequiredNumField
	val func(r Record) float32
}

func NewFloat32Field(val func(r Record) float32, col string) *Float32Field {
	return &Float32Field{
		val:              val,
		RequiredNumField: RequiredNumField{col: col},
	}
}

func (i *Float32Field) Add(r Record) {
	i.vals = append(i.vals, i.val(r))
}

type Float32OptionalField struct {
	OptionalNumField
	val func(r Record) *float32
}

func NewFloat32OptionalField(val func(r Record) *float32, col string) *Float32OptionalField {
	return &Float32OptionalField{
		val:              val,
		OptionalNumField: OptionalNumField{col: col},
	}
}

func (i *Float32OptionalField) Add(r Record) {
	v := i.val(r)
	if v != nil {
		i.vals = append(i.vals, *v)
		i.defs = append(i.defs, 1)
	} else {
		i.defs = append(i.defs, 0)
	}
}

type OptionalBoolField struct {
	vals []bool
	defs []int64
	col  string
	val  func(r Record) *bool
}

func NewOptionalBoolField(val func(r Record) *bool, col string) *OptionalBoolField {
	return &OptionalBoolField{
		val: val,
		col: col,
	}
}

func (f *OptionalBoolField) Add(r Record) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.defs = append(f.defs, 1)
	} else {
		f.defs = append(f.defs, 0)
	}
}

func (f *OptionalBoolField) Write(w io.Writer, meta *schema.Metadata, pos int) error {
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

type RequiredStringField struct {
	vals []string
	col  string
	val  func(r Record) string
}

func NewStringField(val func(r Record) string, col string) *RequiredStringField {
	return &RequiredStringField{
		val: val,
		col: col,
	}
}

func (f *RequiredStringField) Add(r Record) {
	f.vals = append(f.vals, f.val(r))
}

func (f *RequiredStringField) Write(w io.Writer, meta *schema.Metadata, pos int) error {
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
