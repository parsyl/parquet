package rle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	mask1 = uint64(0x7F)
	mask2 = uint64(0x80)
)

type RLE struct {
	baos                      []byte
	bitWidth                  int32
	packBuffer                []byte
	previousValue             int64
	bufferedValues            []int64
	numBufferedValues         int
	repeatCount               int
	bitPackedGroupCount       int
	bitPackedRunHeaderPointer int
	toBytesCalled             bool
}

func New(width int32) *RLE {
	r := &RLE{
		baos:           []byte{},
		bitWidth:       width,
		packBuffer:     make([]byte, int(width)),
		bufferedValues: make([]int64, 8),
	}
	r.reset(false)
	return r
}

func (r *RLE) reset(clearBuf bool) {
	if clearBuf {
		r.baos = []byte{}
	}

	r.previousValue = 0
	r.numBufferedValues = 0
	r.repeatCount = 0
	r.bitPackedGroupCount = 0
	r.bitPackedRunHeaderPointer = -1
	r.toBytesCalled = false
}

func (r *RLE) Write(value int64) {
	if value == r.previousValue {
		// keep track of how many times we've seen this value
		// consecutively
		r.repeatCount++

		if r.repeatCount >= 8 {
			// we've seen this at least 8 times, we're
			// certainly going to write an rle-run,
			// so just keep on counting repeats for now
			return
		}
	} else {
		// This is a new value, check if it signals the end of
		// an rle-run
		if r.repeatCount >= 8 {
			// it does! write an rle-run
			r.writeRLERun()
		}

		// this is a new value so we've only seen it once
		r.repeatCount = 1
		// start tracking this value for repeats
		r.previousValue = value
	}

	// We have not seen enough repeats to justify an rle-run yet,
	// so buffer this value in case we decide to write a bit-packed-run
	r.bufferedValues[r.numBufferedValues] = value
	r.numBufferedValues++

	if r.numBufferedValues == 8 {
		// we've encountered less than 8 repeated values, so
		// either start a new bit-packed-run or append to the
		// current bit-packed-run
		r.writeOrAppendBitPackedRun()
	}
}

func (r *RLE) writeOrAppendBitPackedRun() {
	if r.bitPackedGroupCount >= 63 {
		// we've packed as many values as we can for this run,
		// end it and start a new one
		r.endPreviousBitPackedRun()
	}

	if r.bitPackedRunHeaderPointer == -1 {
		// this is a new bit-packed-run, allocate a byte for the header
		// and keep a "pointer" to it so that it can be mutated later
		r.baos = append(r.baos, 0) // write a sentinel value
		r.bitPackedRunHeaderPointer = len(r.baos) - 1
	}

	r.pack()
	r.baos = append(r.baos, r.packBuffer...)

	// empty the buffer, they've all been written
	r.numBufferedValues = 0

	// clear the repeat count, as some repeated values
	// may have just been bit packed into this run
	r.repeatCount = 0

	r.bitPackedGroupCount++
}

func (r *RLE) endPreviousBitPackedRun() {
	if r.bitPackedRunHeaderPointer == -1 {
		// we're not currently in a bit-packed-run
		return
	}

	// create bit-packed-header, which needs to fit in 1 byte
	bitPackHeader := byte((r.bitPackedGroupCount << 1) | 1)

	// update this byte
	r.baos[r.bitPackedRunHeaderPointer] = bitPackHeader

	// mark that this run is over
	r.bitPackedRunHeaderPointer = -1

	// reset the number of groups
	r.bitPackedGroupCount = 0
}

func (r *RLE) writeRLERun() error {
	r.endPreviousBitPackedRun()
	// write the rle-header (lsb of 0 signifies a rle run)
	r.baos = append(r.baos, r.leb128(r.repeatCount<<1)...)
	// write the repeated-value
	x, err := r.writeIntLittleEndianPaddedOnBitWidth(r.previousValue, r.bitWidth)
	if err != nil {
		return err
	}
	r.baos = append(r.baos, x...)

	// reset the repeat count
	r.repeatCount = 0

	// throw away all the buffered values, they were just repeats and they've been written
	r.numBufferedValues = 0
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
	} else if r.numBufferedValues > 0 {
		for i := r.numBufferedValues; i < 8; i++ {
			r.bufferedValues[i] = 0
		}
		r.writeOrAppendBitPackedRun()
		r.endPreviousBitPackedRun()
	} else {
		r.endPreviousBitPackedRun()
	}

	r.toBytesCalled = true
	var b bytes.Buffer
	binary.Write(&b, binary.LittleEndian, int32(len(r.baos)))
	return append(b.Bytes(), r.baos...)
}

func (r *RLE) pack() {
	r.packBuffer = []byte{
		(byte(r.bufferedValues[0]&1) |
			byte((r.bufferedValues[1]&1)<<1) |
			byte((r.bufferedValues[2]&1)<<2) |
			byte((r.bufferedValues[3]&1)<<3) |
			byte((r.bufferedValues[4]&1)<<4) |
			byte((r.bufferedValues[5]&1)<<5) |
			byte((r.bufferedValues[6]&1)<<6) |
			byte((r.bufferedValues[7]&1)<<7)) & 255,
	}
}

// Read reads the RLE encoded definition levels
func (r *RLE) Read(in io.Reader) ([]int64, int, error) {
	var out []int64
	var l uint16
	if err := binary.Read(in, binary.LittleEndian, &l); err != nil {
		return out, 0, err
	}

	fmt.Println("making buf", l)
	buf := make([]byte, l)
	if _, err := in.Read(buf); err != nil {
		return out, 0, err
	}

	rr := bytes.NewReader(buf)

	var header uint64
	var vals []int64
	var err error
	for rr.Len() > 0 {
		header, err = readUint64(rr)
		fmt.Printf("header: %d, %d\n", header, header&1)
		if err != nil {
			return out, 0, err
		}
		if header&1 == 0 {
			vals, err = readRLE(rr, header, uint64(r.bitWidth))
			if err != nil {
				return out, 0, err
			}
			out = append(out, vals...)
		} else {
			vals, err = readRLEBitPacked(rr, header, uint64(r.bitWidth))
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
	fmt.Println("read bit packed", count)
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
