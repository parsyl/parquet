package parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
	sch "github.com/parsyl/parquet/generated"
)

type RequiredField struct {
	col string
}

func NewRequiredField(col string) RequiredField {
	return RequiredField{col: col}
}

func (f *RequiredField) DoWrite(w io.Writer, meta *Metadata, vals []byte, count int) error {
	compressed := snappy.Encode(nil, vals)
	if err := meta.WritePageHeader(w, f.col, len(vals), len(compressed), count); err != nil {
		return err
	}

	_, err := w.Write(compressed)
	return err
}

func (f *RequiredField) DoRead(r io.ReadSeeker, meta *Metadata, pos Position) (io.Reader, []int, error) {
	var nRead int
	var out []byte
	var sizes []int
	for nRead < pos.N {
		ph, err := meta.PageHeader(r)
		if err != nil {
			return nil, nil, err
		}

		sizes = append(sizes, int(ph.DataPageHeader.NumValues))

		var data []byte
		if pos.Codec == sch.CompressionCodec_SNAPPY {
			compressed := make([]byte, ph.CompressedPageSize)
			if _, err := r.Read(compressed); err != nil {
				return nil, nil, err
			}

			data, err = snappy.Decode(nil, compressed)
			if err != nil {
				return nil, nil, err
			}
		} else if pos.Codec == sch.CompressionCodec_UNCOMPRESSED {
			data = make([]byte, ph.UncompressedPageSize)
			if _, err := r.Read(data); err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("unsupported column chunk codec: %s", pos.Codec)
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
	Defs []int64
	col  string
}

func NewOptionalField(col string) OptionalField {
	return OptionalField{col: col}
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

	err := WriteLevels(wc, f.Defs)
	if err != nil {
		return err
	}

	if _, err := wc.Write(vals); err != nil {
		return err
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := meta.WritePageHeader(w, f.col, int(wc.n), len(compressed), len(f.Defs)); err != nil {
		return err
	}

	_, err = w.Write(compressed)
	return err
}

func (f *OptionalField) DoRead(r io.ReadSeeker, meta *Metadata, pos Position) (io.Reader, []int, error) {
	var nRead int
	var out []byte
	var sizes []int
	for nRead < pos.N {
		ph, err := meta.PageHeader(r)
		if err != nil {
			return nil, nil, err
		}

		var data []byte
		if pos.Codec == sch.CompressionCodec_SNAPPY {
			compressed := make([]byte, ph.CompressedPageSize)
			if _, err := r.Read(compressed); err != nil {
				return nil, nil, err
			}

			data, err = snappy.Decode(nil, compressed)
			if err != nil {
				return nil, nil, err
			}
		} else if pos.Codec == sch.CompressionCodec_UNCOMPRESSED {
			data = make([]byte, ph.UncompressedPageSize)
			if _, err := r.Read(data); err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("unsupported column chunk codec: %s", pos.Codec)
		}

		defs, l, err := ReadLevels(bytes.NewBuffer(data))

		if err != nil {
			return nil, nil, err
		}
		f.Defs = append(f.Defs, defs...)
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

func NewWriteCounter(w io.Writer) *writeCounter {
	return &writeCounter{w: w}
}

func (w *writeCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

type readCounter struct {
	n int64
	r io.ReadSeeker
}

func (r *readCounter) Seek(o int64, w int) (int64, error) {
	return r.r.Seek(o, w)
}

func (r *readCounter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}
