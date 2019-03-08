package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
)

const (
	mask1 = uint64(0x7F)
	mask2 = uint64(0x80)
)

// WriteLevels writes vals to w as RLE encoded data
func WriteLevels(w io.Writer, levels []int64) error {
	var max uint32
	if len(levels) > 0 {
		max = 1
	}

	rle := writeRLE(levels, int32(bits.Len32(max)))
	var out []byte
	binary.Write(w, binary.LittleEndian, int32(len(rle)))
	out = append(out, rle...)
	_, err := w.Write(out)
	return err
}

func writeRLE(levels []int64, width int32) []byte {
	l := len(levels)
	var i, reps int
	var out, val []byte
	for i < l {
		reps = i + 1
		for reps < l && levels[reps] == levels[i] {
			reps++
		}

		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, levels[i])
		val = buf.Bytes()
		out = append(out, append(header(uint64(reps-i)), val[0:(width+7)/8]...)...)
		i = reps
	}
	return out
}

func header(n uint64) []byte {
	n <<= 1
	l := (bits.Len64(n) + 6) / 7
	if l == 0 {
		return make([]byte, 1)
	}
	out := make([]byte, l)
	tmp := n
	for i := 0; i < int(l); i++ {
		out[i] = (byte(tmp & mask1)) | byte(mask2)
		tmp = tmp >> 7
	}
	out[l-1] &= byte(mask1)
	return out
}

// ReadLevels reads the RLE encoded definition levels
func ReadLevels(in io.Reader) ([]int64, int, error) {
	width := uint64(bits.Len64(1)) //TODO: figure out if this is correct
	var out []int64
	var l int32
	if err := binary.Read(in, binary.LittleEndian, &l); err != nil {
		return out, 0, err
	}

	buf := make([]byte, l)
	if _, err := in.Read(buf); err != nil {
		return out, 0, err
	}

	r := bytes.NewReader(buf)

	var header uint64
	var vals []int64
	var err error
	for r.Len() > 0 {
		header, err = readUint64(r)
		fmt.Printf("header: %d, %d\n", header, header&1)
		if err != nil {
			return out, 0, err
		}
		if header&1 == 0 {
			vals, err = readRLE(r, header, width)
			if err != nil {
				return out, 0, err
			}
			out = append(out, vals...)

		} else {
			vals, err = readRLEBitPacked(r, header, width)
			if err != nil {
				return out, 0, err
			}
			out = append(out, vals...)
		}
	}
	return out, int(l + 4), nil
}

func readRLEBitPacked(r io.Reader, header, width uint64) ([]int64, error) {
	count := (header >> 1) * 8
	if width == 0 {
		return make([]int64, count), nil
	}

	byteCount := (width * count) / 8
	rawBytes := make([]byte, byteCount)
	if _, err := r.Read(rawBytes); err != nil {
		return nil, err
	}

	current := 0
	data := uint64(rawBytes[current])
	mask := uint64((1 << width) - 1)
	left := uint64(8)
	right := uint64(0)
	out := make([]int64, 0, count)
	total := uint64(len(rawBytes) * 8)
	for total >= width {
		if right >= 8 {
			right -= 8
			left -= 8
			data >>= 8
		} else if left-right >= width {
			out = append(out, int64((data>>right)&mask))
			total -= width
			right += width
		} else if current+1 < len(rawBytes) {
			current++
			data |= uint64(rawBytes[current] << left)
			left += 8
		}
	}
	return out, nil
}

func readRLE(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	count := header >> 1
	zeroData := make([]byte, 4)
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if _, err := r.Read(data); err != nil {
		return nil, err
	}

	data = append(data, zeroData[len(data):]...)
	value := int64(binary.LittleEndian.Uint32(data))
	out := make([]int64, count)
	for i := 0; i < int(count); i++ {
		out[i] = value
	}
	return out, nil
}

func readUint64(r io.Reader) (uint64, error) {
	var err error
	var out, shift, x uint64
	b := make([]byte, 1)
	for {
		_, err = r.Read(b)
		if err != nil {
			return out, err
		}
		x = uint64(b[0])
		out |= (x & mask1) << shift
		if (x & mask2) == 0 {
			return out, nil
		}
		shift += 7
	}
}
