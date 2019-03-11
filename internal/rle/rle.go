package rle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/parsyl/parquet/internal/bitpack"
)

const (
	mask1 = uint64(0x7F)
	mask2 = uint64(0x80)
)

type RLE struct {
	// TODO: make out a buffer?
	out           *writeBuffer
	bitWidth      int32
	packBuf       []byte
	prev          int64
	valBuf        []int64
	bufCount      int
	repeatCount   int
	groupCount    int
	headerPointer int
}

func New(width int32, size int) *RLE {
	r := &RLE{
		out:           newWriteBuffer(size),
		bitWidth:      width,
		packBuf:       make([]byte, int(width)),
		valBuf:        make([]int64, 8),
		headerPointer: -1,
	}
	return r
}

func (r *RLE) Write(value int64) {
	if value == r.prev {
		r.repeatCount++
		if r.repeatCount >= 8 {
			return
		}
	} else {
		if r.repeatCount >= 8 {
			r.writeRLERun()
		}
		r.repeatCount = 1
		r.prev = value
	}
	r.valBuf[r.bufCount] = value
	r.bufCount++

	if r.bufCount == 8 {
		r.writeOrAppendBitPackedRun()
	}
}

func (r *RLE) writeOrAppendBitPackedRun() {
	if r.groupCount >= 63 {
		r.endPreviousBitPackedRun()
	}

	if r.headerPointer == -1 {
		r.out.Write([]byte{0})
		r.headerPointer = r.out.Size() - 1
	}

	r.out.Write(bitpack.Pack(int(r.bitWidth), r.valBuf))
	r.bufCount = 0
	r.repeatCount = 0
	r.groupCount++
}

func (r *RLE) endPreviousBitPackedRun() {
	if r.headerPointer == -1 {
		return
	}

	bitPackHeader := byte((r.groupCount << 1) | 1)
	r.out.WriteAt([]byte{bitPackHeader}, r.headerPointer)
	r.headerPointer = -1
	r.groupCount = 0
}

func (r *RLE) writeRLERun() error {
	r.endPreviousBitPackedRun()
	r.out.Write(r.leb128(r.repeatCount << 1))
	x, err := r.writeIntLittleEndianPaddedOnBitWidth(r.prev, r.bitWidth)
	if err != nil {
		return err
	}
	r.out.Write(x)
	r.repeatCount = 0
	r.bufCount = 0
	return nil
}

func (r *RLE) writeIntLittleEndianPaddedOnBitWidth(v int64, bitWidth int32) ([]byte, error) {
	bytesWidth := (bitWidth + 7) / 8
	switch bytesWidth {
	case 0:
		return nil, nil
	case 1:
		return []byte{
			byte(uint(v>>0) & 0xFF),
		}, nil
	case 2:
		return []byte{
			byte(uint(v>>0) & 0xFF),
			byte(uint(v>>8) & 0xFF),
		}, nil
	case 3:
		return []byte{
			byte(uint(v>>0) & 0xFF),
			byte(uint(v>>8) & 0xFF),
			byte(uint(v>>16) & 0xFF),
		}, nil
	case 4:
		return []byte{
			byte(uint(v>>0) & 0xFF),
			byte(uint(v>>8) & 0xFF),
			byte(uint(v>>16) & 0xFF),
			byte(uint(v>>24) & 0xFF),
		}, nil
	default:
		return nil, fmt.Errorf("Encountered value (%d) that requires more than 4 bytes", v)
	}
}

func (r *RLE) leb128(value int) []byte {
	var out []byte
	for (value & 0xFFFFFF80) != 0 {
		out = append(out, byte((value&0x7F)|0x80))
		value = int(uint(value) >> 7)
	}
	return append(out, byte(value&0x7F))
}

func (r *RLE) Bytes() []byte {
	if r.repeatCount >= 8 {
		r.writeRLERun()
	} else if r.bufCount > 0 {
		for i := r.bufCount; i < 8; i++ {
			r.valBuf[i] = 0
		}
		r.writeOrAppendBitPackedRun()
		r.endPreviousBitPackedRun()
	} else {
		r.endPreviousBitPackedRun()
	}

	var b bytes.Buffer
	binary.Write(&b, binary.LittleEndian, int32(r.out.Size()))
	return append(b.Bytes(), r.out.Bytes()...)
}

// Read reads the RLE encoded definition levels
func (r *RLE) Read(in io.Reader) ([]int64, int, error) {
	var out []int64
	var length int32
	if err := binary.Read(in, binary.LittleEndian, &length); err != nil {
		return out, 0, err
	}

	buf := make([]byte, length)
	if _, err := in.Read(buf); err != nil {
		return nil, 0, err
	}

	rr := bytes.NewReader(buf)
	var header uint64
	var vals []int64
	var err error
	for rr.Len() > 0 {
		header, err = readLEB128(rr)
		if err != nil {
			return nil, 0, err
		}
		if header&1 == 0 {
			vals, err = readRLE(rr, header, uint64(r.bitWidth))
			if err != nil {
				return nil, 0, err
			}
			out = append(out, vals...)
		} else {
			vals, err = readRLEBitPacked(rr, header, uint64(r.bitWidth))
			if err != nil {
				return nil, 0, err
			}
			out = append(out, vals...)
		}
	}
	return out, int(length) + 4, nil
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

	var out []int64
	for len(rawBytes) > 0 {
		out = append(out, bitpack.Unpack(int(width), rawBytes[:width])...)
		rawBytes = rawBytes[int(width):]
	}

	return out, nil
}

func readRLE(r io.Reader, header uint64, bitWidth uint64) ([]int64, error) {
	count := header >> 1
	value, err := readIntLittleEndianPaddedOnBitWidth(r, int(bitWidth))
	if err != nil {
		return nil, err
	}

	out := make([]int64, count)
	for i := 0; i < int(count); i++ {
		out[i] = int64(value)
	}
	return out, nil
}

func readIntLittleEndianPaddedOnBitWidth(in io.Reader, bitWidth int) (uint64, error) {
	bytesWidth := (bitWidth + 7) / 8
	switch bytesWidth {
	case 0:
		return 0, nil
	case 1:
		return readIntLittleEndianOnOneByte(in)
	case 2:
		return readIntLittleEndianOnTwoBytes(in)
	case 3:
		return readIntLittleEndianOnThreeBytes(in)
	case 4:
		return readIntLittleEndian(in)
	default:
		return 0, fmt.Errorf("Encountered bitWidth (%d) that requires more than 4 bytes", bitWidth)
	}
}

func readIntLittleEndianOnOneByte(in io.Reader) (uint64, error) {
	b := make([]byte, 1)
	_, err := in.Read(b)
	if err != nil {
		return 0, err
	}
	if b[0] < 0 {
		return 0, io.EOF
	}
	return uint64(b[0]), nil
}

func readIntLittleEndianOnTwoBytes(in io.Reader) (uint64, error) {
	b := make([]byte, 2)
	_, err := in.Read(b)
	if err != nil {
		return 0, err
	}
	if b[0] < 0 {
		return 0, io.EOF
	}

	if (b[0] | b[1]) < 0 {
		return 0, io.EOF
	}
	return (uint64(b[1]) << 8) + (uint64(b[0]) << 0), nil
}

func readIntLittleEndianOnThreeBytes(in io.Reader) (uint64, error) {
	b := make([]byte, 3)
	_, err := in.Read(b)
	if err != nil {
		return 0, err
	}

	if (b[0] | b[1] | b[2]) < 0 {
		return 0, io.EOF
	}

	return (uint64(b[2]) << 16) + (uint64(b[1]) << 8) + (uint64(b[0]) << 0), nil
}

func readIntLittleEndian(in io.Reader) (uint64, error) {
	b := make([]byte, 4)
	_, err := in.Read(b)
	if err != nil {
		return 0, err
	}

	if (b[0] | b[1] | b[2] | b[3]) < 0 {
		return 0, io.EOF
	}

	return (uint64(b[3]) << 24) + (uint64(b[2]) << 16) + (uint64(b[1]) << 8) + (uint64(b[0]) << 0), nil
}

func readLEB128(r io.Reader) (uint64, error) {
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
