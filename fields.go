package parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
	sch "github.com/parsyl/parquet/generated"
)

// RequiredField writes the raw data for required columns
type RequiredField struct {
	col         string
	compression sch.CompressionCodec
}

// NewRequiredField creates a new required field.
func NewRequiredField(col string, opts ...func(*RequiredField)) RequiredField {
	r := RequiredField{col: col, compression: sch.CompressionCodec_SNAPPY}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

func RequiredFieldSnappy(r *RequiredField) {
	r.compression = sch.CompressionCodec_SNAPPY
}

func RequiredFieldUncompressed(r *RequiredField) {
	r.compression = sch.CompressionCodec_UNCOMPRESSED
}

// DoWrite writes the actual raw data.
func (f *RequiredField) DoWrite(w io.Writer, meta *Metadata, vals []byte, count int) error {
	l, cl, vals := compress(f.compression, vals)
	if err := meta.WritePageHeader(w, f.col, l, cl, count, f.compression); err != nil {
		return err
	}

	_, err := w.Write(vals)
	return err
}

func (f *RequiredField) DoRead(r io.ReadSeeker, meta *Metadata, pg Page) (io.Reader, []int, error) {
	var nRead int
	var out []byte
	var sizes []int
	for nRead < pg.N {
		ph, err := meta.PageHeader(r)
		if err != nil {
			return nil, nil, err
		}

		sizes = append(sizes, int(ph.DataPageHeader.NumValues))

		data, err := pageData(r, ph, pg)
		if err != nil {
			return nil, nil, err
		}

		out = append(out, data...)
		nRead += int(ph.DataPageHeader.NumValues)
	}

	return bytes.NewBuffer(out), sizes, nil
}

func (f *RequiredField) Name() string {
	return f.col
}

type OptionalField struct {
	Defs        []int64
	col         string
	compression sch.CompressionCodec
}

func NewOptionalField(col string, opts ...func(*OptionalField)) OptionalField {
	f := OptionalField{col: col, compression: sch.CompressionCodec_SNAPPY}
	for _, opt := range opts {
		opt(&f)
	}
	return f
}

func OptionalFieldSnappy(r *OptionalField) {
	r.compression = sch.CompressionCodec_SNAPPY
}

func OptionalFieldUncompressed(o *OptionalField) {
	o.compression = sch.CompressionCodec_UNCOMPRESSED
}

func (f *OptionalField) Values() int {
	return valsFromDefs(f.Defs)
}

func valsFromDefs(defs []int64) int {
	var out int
	for _, d := range defs {
		if d == 1 {
			out++
		}
	}
	return out
}

func (f *OptionalField) DoWrite(w io.Writer, meta *Metadata, vals []byte, count int) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}
	err := writeLevels(wc, f.Defs)
	if err != nil {
		return err
	}

	wc.Write(vals)
	l, cl, vals := compress(f.compression, buf.Bytes())
	if err := meta.WritePageHeader(w, f.col, l, cl, len(f.Defs), f.compression); err != nil {
		return err
	}

	_, err = w.Write(vals)
	return err
}

func (f *OptionalField) DoRead(r io.ReadSeeker, meta *Metadata, pg Page) (io.Reader, []int, error) {
	var nRead int
	var out []byte
	var sizes []int
	for nRead < pg.N {
		ph, err := meta.PageHeader(r)
		if err != nil {
			return nil, nil, err
		}

		data, err := pageData(r, ph, pg)
		if err != nil {
			return nil, nil, err
		}

		defs, l, err := readLevels(bytes.NewBuffer(data))
		if err != nil {
			return nil, nil, err
		}
		f.Defs = append(f.Defs, defs[:int(ph.DataPageHeader.NumValues)]...)
		sizes = append(sizes, valsFromDefs(defs))
		out = append(out, data[l:]...)
		nRead += int(ph.DataPageHeader.NumValues)
	}
	return bytes.NewBuffer(out), sizes, nil
}

func (f *OptionalField) Name() string {
	return f.col
}

type writeCounter struct {
	n int64
	w io.Writer
}

func (w *writeCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func pageData(r io.ReadSeeker, ph *sch.PageHeader, pg Page) ([]byte, error) {
	var data []byte
	switch pg.Codec {
	case sch.CompressionCodec_SNAPPY:
		compressed := make([]byte, ph.CompressedPageSize)
		if _, err := r.Read(compressed); err != nil {
			return nil, err
		}

		var err error
		data, err = snappy.Decode(nil, compressed)
		if err != nil {
			return nil, err
		}
	case sch.CompressionCodec_UNCOMPRESSED:
		data = make([]byte, ph.UncompressedPageSize)
		if _, err := r.Read(data); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported column chunk codec: %s", pg.Codec)
	}

	return data, nil
}

func compress(codec sch.CompressionCodec, vals []byte) (int, int, []byte) {
	var l, cl int
	switch codec {
	case sch.CompressionCodec_SNAPPY:
		l = len(vals)
		vals = snappy.Encode(nil, vals)
		cl = len(vals)
	case sch.CompressionCodec_UNCOMPRESSED:
		l = len(vals)
		cl = len(vals)
	}
	return l, cl, vals
}
