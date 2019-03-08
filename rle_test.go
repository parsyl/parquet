package parquet_test

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/parsyl/parquet"
	"github.com/stretchr/testify/assert"
)

//go:generate parquetgen -input parquet_test.go -type Person -package parquet_test -output parquet_generated_test.go

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestRLE(t *testing.T) {
	type testCase struct {
		name  string
		input []int64
	}

	testCases := []testCase{
		{
			name:  "various numbers",
			input: []int64{3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 6, 2, 2, 2, 2, 2},
		},
		{
			name:  "1s",
			input: []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name:  "1s",
			input: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			assert.NoError(t, parquet.WriteLevels(&buf, tc.input), tc.name)
			levels, _, err := parquet.ReadLevels(bytes.NewReader(buf.Bytes()))
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.input, levels, tc.name)
		})
	}
}
