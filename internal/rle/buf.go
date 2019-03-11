package rle

type writeBuffer struct {
	d []byte
	i int
}

func newWriteBuffer(size int) *writeBuffer {
	return &writeBuffer{d: make([]byte, size)}
}

func (w *writeBuffer) size() int {
	return w.i
}

func (w *writeBuffer) bytes() []byte {
	return w.d[:w.i]
}

func (w *writeBuffer) write(dat []byte) (int, error) {
	return w.writeAt(dat, w.i)
}

func (w *writeBuffer) writeAt(dat []byte, off int) (int, error) {
	if len(dat)+off > w.i {
		w.i = len(dat) + off
	}

	if off == len(w.d) {
		w.d = append(w.d, dat...)
		return len(dat), nil
	}

	if off+len(dat) >= len(w.d) {
		nd := make([]byte, int(off)+len(dat))
		copy(nd, w.d)
		w.d = nd
	}

	copy(w.d[int(off):], dat)
	return len(dat), nil
}
