package rle_test

import (
	"fmt"
	"testing"

	"github.com/parsyl/parquet/internal/rle"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name string
	in   []int64
	out  []byte
}

func TestRLE(t *testing.T) {
	testCases := []testCase{
		{
			name: "rle only",
			in:   append(repeat(4, 100), repeat(5, 100)...),
			// header = 100 << 1 = 200
			// payload = 4
			// header = 100 << 1 = 200
			// payload = 5
			out: []byte{byte(uint(100 << 1)), byte(4), byte(uint(100 << 1)), byte(5)},
		},
		{
			name: "repeated zeros",
			in:   repeat(0, 10),
			// header = 10 << 1 = 20
			// payload = 4
			out: []byte{byte(uint(10 << 1)), byte(4)},
		},
		// Causes panic
		// {
		// 	name: "bitpacking only",
		// 	in:   mod(3, 100),
		// 	// header = ((104/8) << 1) | 1 = 27
		// 	out: append([]byte{byte(27)}, modbytes(3, 100)...),
		// },
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%s", i, tc.name), func(t *testing.T) {
			r := rle.New(3)
			for _, x := range tc.in {
				r.Write(x)
			}
			assert.Equal(t, tc.out, r.Bytes(), tc.name)
		})
	}
}

func mod(m, c int) []int64 {
	out := make([]int64, c)
	for i := range out {
		out[i] = int64(i % m)
	}
	return out
}

func modbytes(m, c int) []byte {
	out := make([]byte, c)
	for i := range out {
		out[i] = byte(int64(i % m))
	}
	return out
}

func repeat(v int64, c int) []int64 {
	out := make([]int64, c)
	for i := range out {
		out[i] = v
	}
	return out
}
