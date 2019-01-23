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
	IDDefs  []int64
	IDReps  []int64
	Age     []int32
	AgeDefs []int64
	AgeReps []int64

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
		meta: schema.New(
			schema.Field{Name: "id", Type: schema.Int32Type, RepetitionType: schema.RepetitionRequired},
			schema.Field{Name: "age", Type: schema.Int32Type, RepetitionType: schema.RepetitionOptional},
		),
	}

	for _, opt := range opts {
		opt(r)
	}
	return r
}

func Max(m int) func(*Records) {
	return func(r *Records) {
		r.max = m
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

	if err := r.meta.Footer(r.w); err != nil {
		return err
	}

	_, err := r.w.Write([]byte("PAR1"))
	return err
}

func (r *Records) writeID() error {
	pos := r.w.n
	buf := bytes.Buffer{}
	w := &writeCounter{w: &buf}

	for _, i := range r.ID {
		if err := binary.Write(w, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := r.meta.WritePageHeader(r.w, "id", pos, w.n, len(compressed), len(r.ID)); err != nil {
		return err
	}

	_, err := io.Copy(r.w, bytes.NewBuffer(compressed))
	return err
}

func (r *Records) writeAge() error {
	pos := r.w.n
	buf := bytes.Buffer{}
	w := &writeCounter{w: &buf}

	err := writeLevels(w, r.AgeDefs)
	if err != nil {
		return err
	}

	for _, a := range r.Age {
		if err := binary.Write(w, binary.LittleEndian, a); err != nil {
			return err
		}
	}

	compressed := snappy.Encode(nil, buf.Bytes())
	if err := r.meta.WritePageHeader(r.w, "age", pos, w.n, len(compressed), len(r.AgeDefs)); err != nil {
		return err
	}

	_, err = io.Copy(r.w, bytes.NewBuffer(compressed))
	return err
}

func (r *Records) Add(rec Record) {
	if len(r.ID) == r.max {
		if r.records == nil {
			r.records = New(r.w, Max(r.max))
			r.records.meta = r.meta
		}

		r.records.Add(rec)
		return
	}

	r.ID = append(r.ID, rec.ID)
	r.IDDefs = append(r.IDDefs, 1)
	if rec.Age != nil {
		r.Age = append(r.Age, *rec.Age)
		r.AgeDefs = append(r.AgeDefs, 1)
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
