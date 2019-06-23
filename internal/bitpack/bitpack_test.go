package bitpack_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/parsyl/parquet/internal/bitpack"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name  string
	width int
	ints  []uint8
	bytes []byte
}

func TestPackAndUnpack(t *testing.T) {
	testCases := []testCase{
		{
			name:  "width 1",
			width: 1,
			ints:  []uint8{0, 1, 1, 0, 0, 1, 1, 1},
			bytes: getBytes("11100110"),
		},
		{
			name:  "width 2",
			width: 2,
			ints:  []uint8{0, 1, 2, 0, 0, 1, 2, 2},
			bytes: getBytes("00100100", "10100100"),
		},
		{
			name:  "width 3 from apache documentation",
			width: 3,
			ints:  []uint8{0, 1, 2, 3, 4, 5, 6, 7},
			bytes: getBytes("10001000", "11000110", "11111010"),
		},
		{
			name:  "width 4",
			width: 4,
			ints:  []uint8{0, 2, 4, 7, 14, 15, 1, 0},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d %s", i, tc.name), func(t *testing.T) {
			b := bitpack.Pack(tc.width, tc.ints)
			if len(tc.bytes) > 0 {
				assert.Equal(t, tc.bytes, b)
			}
			n := bitpack.Unpack(tc.width, b)
			assert.Equal(t, tc.ints, n[:len(tc.ints)])
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
