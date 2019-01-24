package parquet

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cswank/parquet/schema"
)

// Records reprents a row group
type Records struct {
	fields []Field

	len int

	// records are for subsequent chunks
	records *Records

	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta *schema.Metadata
	w    *writeCounter
}

func New(w io.Writer, opts ...func(*Records)) *Records {
	r := &Records{
		max: 1000,
		w:   &writeCounter{w: w},
		fields: []Field{
			newInt32Field(func(r Record) int32 { return r.ID }, "id"),
			newInt32OptionalField(func(r Record) *int32 { return r.Age }, "age"),
			newInt64Field(func(r Record) int64 { return r.Happiness }, "happiness"),
			newInt64OptionalField(func(r Record) *int64 { return r.Sadness }, "sadness"),
			newStringField(func(r Record) string { return r.Code }, "code"),
			newFloat32Field(func(r Record) float32 { return r.Funkiness }, "funkiness"),
			newFloat32OptionalField(func(r Record) *float32 { return r.Lameness }, "lameness"),
			newOptionalBoolField(func(r Record) *bool { return r.Keen }, "keen"),
		},
		meta: schema.New(
			schema.Field{Name: "id", Type: schema.Int32Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "age", Type: schema.Int32Type, RepetitionType: schema.RepetitionOptional},
			schema.Field{Name: "happiness", Type: schema.Int64Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "sadness", Type: schema.Int64Type, RepetitionType: schema.RepetitionOptional},
			schema.Field{Name: "code", Type: schema.StringType, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "funkiness", Type: schema.Float32Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "lameness", Type: schema.Float32Type, RepetitionType: schema.RepetitionOptional},
			schema.Field{Name: "keen", Type: schema.BoolType, RepetitionType: schema.RepetitionOptional},
		),
	}

	for _, opt := range opts {
		opt(r)
	}
	return r
}

// MaxPageSize is the maximum number of rows in each row groups' page.
func MaxPageSize(m int) func(*Records) {
	return func(r *Records) {
		r.max = m
	}
}

func (r *Records) Write() error {
	if _, err := r.w.Write([]byte("PAR1")); err != nil {
		return err
	}

	for i, f := range r.fields {
		pos := r.w.n
		f.write(r.w, r.meta, pos)

		for child := r.records; child != nil; child = child.records {
			pos := r.w.n
			child.fields[i].write(r.w, r.meta, pos)
		}
	}

	if err := r.meta.Footer(r.w); err != nil {
		return err
	}

	_, err := r.w.Write([]byte("PAR1"))
	return err
}

func (r *Records) Add(rec Record) {
	if r.len == r.max {
		if r.records == nil {
			r.records = New(r.w, MaxPageSize(r.max))
			r.records.meta = r.meta
		}

		r.records.Add(rec)
		return
	}

	for _, f := range r.fields {
		f.add(rec)
	}

	r.len++
}

type Record struct {
	ID        int32
	Age       *int32
	Happiness int64
	Sadness   *int64
	Code      string
	Funkiness float32
	Lameness  *float32
	Keen      *bool
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

// writeLevels writes vals to w as RLE encoded data
func writeLevels(w io.Writer, vals []int64) error {
	var max uint64
	if len(vals) > 0 {
		max = 1
	}

	rleBuf := writeRLE(vals, int32(bitNum(max)))
	res := make([]byte, 0)
	var lenBuf bytes.Buffer
	binary.Write(&lenBuf, binary.LittleEndian, int32(len(rleBuf)))
	res = append(res, lenBuf.Bytes()...)
	res = append(res, rleBuf...)
	_, err := io.Copy(w, bytes.NewBuffer(res))
	return err
}

func writeRLE(vals []int64, bitWidth int32) []byte {
	ln := len(vals)
	i := 0
	res := make([]byte, 0)
	for i < ln {
		j := i + 1
		for j < ln && vals[j] == vals[i] {
			j++
		}
		num := j - i
		header := num << 1
		byteNum := (bitWidth + 7) / 8

		headerBuf := writeUnsignedVarInt(uint64(header))

		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, vals[i])
		valBuf := buf.Bytes()
		rleBuf := make([]byte, int64(len(headerBuf))+int64(byteNum))
		copy(rleBuf[0:], headerBuf)
		copy(rleBuf[len(headerBuf):], valBuf[0:byteNum])
		res = append(res, rleBuf...)
		i = j
	}
	return res
}

func writeUnsignedVarInt(num uint64) []byte {
	byteNum := (bitNum(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
	}
	res := make([]byte, byteNum)

	numTmp := num
	for i := 0; i < int(byteNum); i++ {
		res[i] = byte(numTmp & uint64(0x7F))
		res[i] = res[i] | byte(0x80)
		numTmp = numTmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}

func bitNum(num uint64) uint64 {
	var bitn uint64 = 0
	for ; num != 0; num >>= 1 {
		bitn++
	}
	return bitn
}
