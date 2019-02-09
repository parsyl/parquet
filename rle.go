package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"
)

// WriteLevels writes vals to w as RLE encoded data
func WriteLevels(w io.Writer, levels []int64) error {
	var max uint32
	if len(levels) > 0 {
		max = 1
	}

	rleBuf := writeRLE(levels, int32(bits.Len32(max)))
	res := make([]byte, 0)
	binary.Write(w, binary.LittleEndian, int32(len(rleBuf)))
	res = append(res, rleBuf...)
	_, err := w.Write(res)
	return err
}

func writeRLE(levels []int64, bitWidth int32) []byte {
	ln := len(levels)
	var i int
	var out []byte
	for i < ln {
		j := i + 1
		for j < ln && levels[j] == levels[i] {
			j++
		}
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, levels[i])
		val := buf.Bytes()
		bn := (bitWidth + 7) / 8
		out = append(out, append(header(uint64(j-i)), val[0:bn]...)...)
		i = j
	}
	return out
}

func header(n uint64) []byte {
	n = n << 1
	byteNum := (bits.Len64(n) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
	}
	res := make([]byte, byteNum)
	tmp := n
	for i := 0; i < int(byteNum); i++ {
		res[i] = byte(tmp & uint64(0x7F))
		res[i] = res[i] | byte(0x80)
		tmp = tmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}

// ReadLevels reads the RLE encoded definition levels
func ReadLevels(r io.Reader) ([]int64, int, error) {
	bitWidth := uint64(bits.Len64(1)) //TODO: figure out if this is correct
	res := make([]int64, 0)
	var l int32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return res, 0, err
	}

	buf := make([]byte, l)
	if _, err := r.Read(buf); err != nil {
		return res, 0, err
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header, err := readUint64(newReader)
		if err != nil {
			return res, 0, err
		}
		if header&1 == 0 {
			buf, err := readRLE(newReader, header, bitWidth)
			if err != nil {
				return res, 0, err
			}
			res = append(res, buf...)

		} else {
			buf, err := readBitPacked(newReader, header, bitWidth)
			if err != nil {
				return res, 0, err
			}
			res = append(res, buf...)
		}
	}
	return res, int(l + 4), nil
}

func readBitPacked(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	var err error
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8

	out := make([]int64, 0, cnt)

	if cnt == 0 {
		return out, nil
	}

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			out = append(out, int64(0))
		}
		return out, err
	}
	bytesBuf := make([]byte, byteCnt)
	if _, err = r.Read(bytesBuf); err != nil {
		return out, err
	}

	i := 0
	var cur, used uint64
	neededBits := bitWidth
	left := uint64(8)
	b := uint64(bytesBuf[i])
	for i < len(bytesBuf) {
		if left >= neededBits {
			cur |= ((b >> used) & ((1 << neededBits) - 1)) << (bitWidth - neededBits)
			out = append(out, int64(cur))
			left -= neededBits
			used += neededBits
			neededBits = bitWidth
			cur = 0
			if left <= 0 && i+1 < len(bytesBuf) {
				i += 1
				b = uint64(bytesBuf[i])
				left = 8
				used = 0
			}
		} else {
			cur |= (b >> used) << (bitWidth - neededBits)
			i += 1
			if i < len(bytesBuf) {
				b = uint64(bytesBuf[i])
			}
			neededBits -= left
			left = 8
			used = 0
		}
	}
	return out, err
}

func readRLE(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	var err error
	var out []int64
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = r.Read(data); err != nil {
			return out, err
		}
	}
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := int64(binary.LittleEndian.Uint32(data))
	c := header >> 1
	out = make([]int64, c)
	for i := 0; i < int(c); i++ {
		out[i] = val
	}
	return out, err
}

const (
	mask1 = uint64(0x7F)
	mask2 = uint64(0x80)
)

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
