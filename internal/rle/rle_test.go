package rle_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/inigolabs/parquet/internal/rle"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	width int32
	name  string
	in    []uint8
	out   []byte
	err   error
}

func TestRLE(t *testing.T) {
	testCases := []testCase{
		{
			name:  "rle only",
			width: 3,
			in:    append(repeat(4, 100), repeat(5, 100)...),
		},
		{
			name:  "repeated zeros",
			width: 0,
			in:    repeat(0, 10),
		},
		{
			name:  "odd number of repeated zeros",
			width: 1,
			in:    repeat(0, 17),
		},
		{
			name:  "odd number of repeated ones",
			width: 1,
			in:    repeat(1, 17),
		},
		{
			name:  "bitpacking only",
			width: 3,
			in:    mod(3, 100),
		},
		{
			name:  "more bitpacking only",
			width: 3,
			in:    mod(3, 103),
		},
		{
			name:  "single value",
			width: 1,
			in:    []uint8{1},
		},
		{
			name:  "odd number of non-repeated values",
			width: 1,
			in:    []uint8{1, 0, 1, 1, 0},
		},
		{
			name:  "width 2",
			width: 2,
			in:    []uint8{1, 2, 3},
		},
		{
			name:  "width 3",
			width: 3,
			in:    []uint8{1, 2, 7},
		},
		{
			name:  "width 4",
			width: 4,
			err:   fmt.Errorf("bitwidth 4 is greater than 3 (highest supported)"),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%02d-%s", i, tc.name), func(t *testing.T) {
			r, err := rle.New(tc.width, len(tc.in))
			if tc.err != nil {
				assert.Error(t, tc.err, err)
				return
			}

			if !assert.NoError(t, err) {
				return
			}

			for _, x := range tc.in {
				r.Write(x)
			}
			b := r.Bytes()
			vals, _, err := r.Read(bytes.NewReader(b))
			if assert.NoError(t, err, tc.name) {
				assert.Equal(t, tc.in, vals[:len(tc.in)], tc.name)
			}
		})
	}
}

func mod(m, c int) []uint8 {
	out := make([]uint8, c)
	for i := range out {
		out[i] = uint8(i % m)
	}
	return out
}

func modbytes(m, c int) []byte {
	out := make([]byte, c)
	for i := range out {
		out[i] = byte(uint8(i % m))
	}
	return out
}

func repeat(v uint8, c int) []uint8 {
	out := make([]uint8, c)
	for i := range out {
		out[i] = v
	}
	return out
}
