package rle

type writeBuffer struct {
	d []byte
	i int
}

func newWriteBuffer(size int) *writeBuffer {
	return &writeBuffer{d: make([]byte, size)}
}

func (wb *writeBuffer) Size() int {
	return wb.i
}

func (wb *writeBuffer) Bytes() []byte {
	return wb.d[:wb.i]
}

func (wb *writeBuffer) Write(dat []byte) (int, error) {
	return wb.WriteAt(dat, wb.i)
}

func (wb *writeBuffer) WriteAt(dat []byte, off int) (int, error) {
	if len(dat)+off > wb.i {
		wb.i = len(dat) + off
	}

	if off == len(wb.d) {
		wb.d = append(wb.d, dat...)
		return len(dat), nil
	}

	if off+len(dat) >= len(wb.d) {
		nd := make([]byte, int(off)+len(dat))
		copy(nd, wb.d)
		wb.d = nd
	}

	copy(wb.d[int(off):], dat)
	return len(dat), nil
}
