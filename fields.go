package parquet

import (
	"bytes"
	"compress/gzip"
	"math/bits"
	"strings"

	"fmt"

	"io"

	"github.com/golang/snappy"
	"github.com/parsyl/parquet/internal/fields"
	"github.com/parsyl/parquet/internal/rle"
	sch "github.com/parsyl/parquet/schema"
)

// RequiredField writes the raw data for required columns
type RequiredField struct {
	pth         []string
	compression sch.CompressionCodec
}

// NewRequiredField creates a required field.
func NewRequiredField(pth []string, opts ...func(*RequiredField)) RequiredField {
	r := RequiredField{
		pth:         pth,
		compression: sch.CompressionCodec_SNAPPY,
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

// RequiredFieldSnappy sets the compression for a column to snappy
// It is an optional arg to NewRequiredField
func RequiredFieldSnappy(r *RequiredField) {
	r.compression = sch.CompressionCodec_SNAPPY
}

// RequiredFieldGzip sets the compression for a column to gzip
// It is an optional arg to NewRequiredField
func RequiredFieldGzip(r *RequiredField) {
	r.compression = sch.CompressionCodec_GZIP
}

// RequiredFieldUncompressed sets the compression to none
// It is an optional arg to NewRequiredField
func RequiredFieldUncompressed(r *RequiredField) {
	r.compression = sch.CompressionCodec_UNCOMPRESSED
}

// DoWrite writes the actual raw data.
func (f *RequiredField) DoWrite(w io.Writer, meta *Metadata, vals []byte, count int, stats Stats) error {
	l, cl, vals, err := compress(f.compression, vals)
	if err != nil {
		return err
	}

	if err := meta.WritePageHeader(w, f.pth, l, cl, count, count, 0, 0, f.compression, stats); err != nil {
		return err
	}

	_, err = w.Write(vals)
	return err
}

// DoRead reads the actual raw data.
func (f *RequiredField) DoRead(r io.ReadSeeker, pg Page) (io.Reader, []int, error) {
	var nRead int
	var out []byte
	var sizes []int
	for nRead < pg.N {
		ph, err := PageHeader(r)
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

// Name returns the column name of this field
func (f *RequiredField) Name() string {
	return strings.Join(f.pth, ".")
}

// Path returns the path of this field
func (f *RequiredField) Path() []string {
	return f.pth
}

// MaxLevel holds the maximum definition and
// repeptition level for a given field.
type MaxLevel struct {
	Def uint8
	Rep uint8
}

// OptionalField is any exported field in a
// struct that is a pointer.
type OptionalField struct {
	Defs           []uint8
	Reps           []uint8
	pth            []string
	MaxLevels      MaxLevel
	compression    sch.CompressionCodec
	RepetitionType FieldFunc
	Types          []int
	repeated       bool
}

func getRepetitionTypes(in []int) fields.RepetitionTypes {
	out := make([]fields.RepetitionType, len(in))
	for i, x := range in {
		out[i] = fields.RepetitionType(x)
	}
	return fields.RepetitionTypes(out)
}

// NewOptionalField creates an optional field
func NewOptionalField(pth []string, types []int, opts ...func(*OptionalField)) OptionalField {
	rts := getRepetitionTypes(types)
	f := OptionalField{
		pth:         pth,
		compression: sch.CompressionCodec_SNAPPY,
		MaxLevels: MaxLevel{
			Def: rts.MaxDef(),
			Rep: rts.MaxRep(),
		},
		RepetitionType: fieldFuncs[types[len(types)-1]],
		Types:          types,
		repeated:       rts.MaxRep() > 0,
	}

	for _, opt := range opts {
		opt(&f)
	}
	return f
}

// OptionalFieldSnappy sets the compression for a column to snappy
// It is an optional arg to NewOptionalField
func OptionalFieldSnappy(r *OptionalField) {
	r.compression = sch.CompressionCodec_SNAPPY
}

// OptionalFieldGzip sets the compression for a column to gzip
// It is an optional arg to NewOptionalField
func OptionalFieldGzip(r *OptionalField) {
	r.compression = sch.CompressionCodec_GZIP
}

// OptionalFieldUncompressed sets the compression to none
// It is an optional arg to NewOptionalField
func OptionalFieldUncompressed(o *OptionalField) {
	o.compression = sch.CompressionCodec_UNCOMPRESSED
}

// Values reads the definition levels and uses them
// to return the values from the page data.
func (f *OptionalField) Values() int {
	return f.valsFromDefs(f.Defs, uint8(f.MaxLevels.Def))
}

func (f *OptionalField) valsFromDefs(defs []uint8, max uint8) int {
	var out int
	for _, d := range defs {
		if d == max {
			out++
		}
	}
	return out
}

// DoWrite is called by all optional field types to write the definition levels
// and raw data to the io.Writer
func (f *OptionalField) DoWrite(w io.Writer, meta *Metadata, vals []byte, count int, stats Stats) error {
	buf := bytes.Buffer{}
	wc := &writeCounter{w: &buf}
	err := writeLevels(wc, f.Defs, int32(bits.Len(uint(f.MaxLevels.Def))))
	if err != nil {
		return err
	}

	defLen := wc.n

	if f.repeated {
		err := writeLevels(wc, f.Reps, int32(bits.Len(uint(f.MaxLevels.Rep))))
		if err != nil {
			return err
		}
	}

	repLen := wc.n - defLen

	wc.Write(vals)
	l, cl, vals, err := compress(f.compression, buf.Bytes())
	if err != nil {
		return err
	}

	if err := meta.WritePageHeader(w, f.pth, l, cl, len(f.Defs), count, defLen, repLen, f.compression, stats); err != nil {
		return err
	}
	_, err = w.Write(vals)
	return err
}

// DoRead is called by all optional fields.  It reads the definition levels and uses
// them to interpret the raw data.
func (f *OptionalField) DoRead(r io.ReadSeeker, pg Page) (io.Reader, []int, error) {
	var nRead int
	var out []byte
	var sizes []int
	var rc *readCounter

	for nRead < pg.Size {
		rc = &readCounter{r: r}
		ph, err := PageHeader(rc)
		if err != nil {
			return nil, nil, err
		}

		data, err := pageData(rc, ph, pg)
		if err != nil {
			return nil, nil, err
		}

		defs, l, err := readLevels(bytes.NewBuffer(data), int32(bits.Len(uint(f.MaxLevels.Def))))
		if err != nil {
			return nil, nil, err
		}

		f.Defs = append(f.Defs, defs[:int(ph.DataPageHeader.NumValues)]...)
		if f.repeated {
			reps, l2, err := readLevels(bytes.NewBuffer(data[l:]), int32(bits.Len(uint(f.MaxLevels.Rep))))
			if err != nil {
				return nil, nil, err
			}
			l += l2
			f.Reps = append(f.Reps, reps[:int(ph.DataPageHeader.NumValues)]...)
		}

		n := f.valsFromDefs(defs, uint8(f.MaxLevels.Def))
		sizes = append(sizes, n)
		out = append(out, data[l:]...)
		nRead += int(rc.n)
	}
	return bytes.NewBuffer(out), sizes, nil
}

// Name returns the column name of this field
func (f *OptionalField) Name() string {
	return strings.Join(f.pth, ".")
}

// Path returns the path of this field
func (f *OptionalField) Path() []string {
	return f.pth
}

// writeCounter keeps track of the number of bytes written
// it is used for calls to binary.Write, which does not
// return the number of bytes written.
type writeCounter struct {
	n int64
	w io.Writer
}

// Write makes writeCounter an io.Writer
func (w *writeCounter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

// readCounter keeps track of the number of bytes written
// it is used for calls to binary.Write.
type readCounter struct {
	n int64
	r io.Reader
}

// Write makes writeCounter an io.Writer
func (r *readCounter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func pageData(r io.Reader, ph *sch.PageHeader, pg Page) ([]byte, error) {
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
	case sch.CompressionCodec_GZIP:
		var buf bytes.Buffer
		_, err := io.CopyN(&buf, r, int64(ph.CompressedPageSize))
		if err != nil {
			return nil, err
		}

		zr, err := gzip.NewReader(&buf)
		if err != nil {
			return nil, err
		}

		data, err = io.ReadAll(zr)
		if err != nil {
			return nil, err
		}

		if err := zr.Close(); err != nil {
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

func compress(codec sch.CompressionCodec, vals []byte) (int, int, []byte, error) {
	var err error
	l := len(vals)
	switch codec {
	case sch.CompressionCodec_SNAPPY:
		vals = snappy.Encode(nil, vals)
	case sch.CompressionCodec_GZIP:
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		if err != nil {
			return l, 0, vals, err
		}

		_, err = zw.Write(vals)
		if err != nil {
			return l, 0, vals, err
		}

		err = zw.Close()
		if err != nil {
			return l, 0, vals, err
		}

		vals = buf.Bytes()
	}
	return l, len(vals), vals, err
}

// writeLevels writes vals to w as RLE/bitpack encoded data
func writeLevels(w io.Writer, levels []uint8, width int32) error {
	enc, _ := rle.New(width, len(levels)) //TODO: len(levels) is probably too big.  Chop it down a bit?
	for _, l := range levels {
		enc.Write(l)
	}
	_, err := w.Write(enc.Bytes())
	return err
}

// readLevels reads the RLE/bitpack encoded definition and repetition levels
func readLevels(in io.Reader, width int32) ([]uint8, int, error) {
	var out []uint8
	dec, _ := rle.New(width, 0)
	out, n, err := dec.Read(in)
	if err != nil {
		return nil, 0, err
	}

	return out, n, nil
}
