package parquet

import (
	"io"

	"github.com/parsyl/parquet/internal/rle"
)

// writeLevels writes vals to w as RLE encoded data
func writeLevels(w io.Writer, levels []int64) error {
	enc := rle.New(1, len(levels))
	for _, l := range levels {
		enc.Write(l)
	}
	_, err := w.Write(enc.Bytes())
	return err
}

// readLevels reads the RLE encoded definition levels
func readLevels(in io.Reader) ([]int64, int, error) {
	var out []int64
	dec := rle.New(1, 0)
	out, n, err := dec.Read(in)
	if err != nil {
		return nil, 0, err
	}

	return out, n, nil
}
