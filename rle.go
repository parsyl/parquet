package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
)

// WriteLevels writes vals to w as RLE encoded data
func WriteLevels(w io.Writer, vals []int64) error {
	var max uint64
	if len(vals) > 0 {
		max = 1
	}

	rleBuf := writeRLE(vals, int32(bitNum(max)))
	res := make([]byte, 0)
	binary.Write(w, binary.LittleEndian, int32(len(rleBuf)))
	res = append(res, rleBuf...)
	_, err := w.Write(res)
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
	var bitn uint64
	for ; num != 0; num >>= 1 {
		bitn++
	}
	return bitn
}

// ReadLevels reads the RLE encoded definition levels
func ReadLevels(r io.Reader) ([]int64, int, error) {
	bitWidth := bitNum(1) //TODO: figure out if this is correct

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
		header, err := readUnsignedVarInt(newReader)
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

	res := make([]int64, 0, cnt)

	if cnt == 0 {
		return res, nil
	}

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, int64(0))
		}
		return res, err
	}
	bytesBuf := make([]byte, byteCnt)
	if _, err = r.Read(bytesBuf); err != nil {
		return res, err
	}

	i := 0
	var resCur uint64 = 0
	var resCurNeedBits uint64 = bitWidth
	var used uint64 = 0
	var left uint64 = 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= uint64(((uint64(b) >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(bitWidth-resCurNeedBits))
			res = append(res, int64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i += 1
				b = bytesBuf[i]
				left = 8
				used = 0
			}

		} else {
			resCur |= uint64((uint64(b) >> uint64(used)) << uint64(bitWidth-resCurNeedBits))
			i += 1
			if i < len(bytesBuf) {
				b = bytesBuf[i]
			}
			resCurNeedBits -= left
			left = 8
			used = 0
		}
	}
	return res, err
}

func readRLE(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	var err error
	var res []int64
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = r.Read(data); err != nil {
			return res, err
		}
	}
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := int64(binary.LittleEndian.Uint32(data))
	res = make([]int64, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res, err
}

func readUnsignedVarInt(r io.Reader) (uint64, error) {
	var err error
	var res uint64 = 0
	var shift uint64 = 0
	for {
		b := make([]byte, 1)
		_, err := r.Read(b)
		if err != nil {
			break
		}
		res |= ((uint64(b[0]) & uint64(0x7F)) << uint64(shift))
		if (b[0] & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return res, err
}
