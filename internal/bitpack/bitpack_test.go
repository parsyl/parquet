package bitpack_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/parsyl/parquet/internal/bitpack"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name     string
	input    []int64
	expected []byte
}

func TestBitpack(t *testing.T) {
	testCases := []testCase{
		{
			name:     "from documentation",
			input:    []int64{0, 1, 2, 3, 4, 5, 6, 7},
			expected: getBytes("10001000", "11000110", "11111010"),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%s", i, tc.name), func(t *testing.T) {
			b := bitpack.Pack(tc.input)
			assert.Equal(t, tc.expected, b)
		})
	}
}

func getBytes(vals ...string) []byte {
	out := make([]byte, len(vals))
	for i, s := range vals {
		x, _ := strconv.ParseInt(s, 2, 16)
		out[i] = byte(x)
	}
	return out
}
