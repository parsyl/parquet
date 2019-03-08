package rle

import "fmt"

type BytePacker interface {
	Pack([]int64, int, []byte, int)
}

type RLE struct {
	packer                    BytePacker
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
		r.baos = append(r.baos, byte(0)) // write a sentinel value
		r.bitPackedRunHeaderPointer = len(r.baos) - 1
	}

	r.packer.Pack(r.bufferedValues, 0, r.packBuffer, 0)
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

	r.baos = r.leb128(r.repeatCount<<1, r.baos)
	// write the repeated-value
	x, err := r.writeIntLittleEndianPaddedOnBitWidth(r.previousValue, r.bitWidth)
	if err != nil {
		return err
	}
	r.baos = append(r.baos, x...)
	//BytesUtils.writeIntLittleEndianPaddedOnBitWidth(baos, previousValue, bitWidth);

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

func (r *RLE) leb128(value int, out []byte) []byte {
	for (value & 0xFFFFFF80) != 0 {
		out = append(out, byte((value&0x7F)|0x80))
		value = int(uint(value) >> 7)
	}
	return append(out, byte(value&0x7F))
}

func (r *RLE) Bytes() []byte {
	return r.baos
}
