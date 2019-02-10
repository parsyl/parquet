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
			buf, err := readRLEBitPacked(newReader, header, bitWidth)
			if err != nil {
				return res, 0, err
			}
			res = append(res, buf...)
		}
	}
	return res, int(l + 4), nil
}

func readRLEBitPacked(r io.Reader, header, width uint64) ([]int64, error) {
	nGroups := header >> 1
	count := nGroups * 8
	if width == 0 {
		return make([]int64, count), nil
	}

	byteCount := (width * count) / 8
	rawBytes := make([]byte, byteCount)
	if _, err := r.Read(rawBytes); err != nil {
		return nil, err
	}

	currentByte := 0
	data := uint64(rawBytes[currentByte])
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
		} else if currentByte+1 < len(rawBytes) {
			currentByte++
			data |= uint64(rawBytes[currentByte] << left)
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
