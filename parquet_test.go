package parquet_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

//go:generate parquetgen -input parquet_test.go -type Person -package parquet_test -output parquet_generated_test.go

type Being struct {
	ID  int32  `parquet:"id"`
	Age *int32 `parquet:"age"`
}

type Person struct {
	Being
	Happiness   int64    `parquet:"happiness"`
	Sadness     *int64   `parquet:"sadness"`
	Code        *string  `parquet:"code"`
	Funkiness   float32  `parquet:"funkiness"`
	Lameness    *float32 `parquet:"lameness"`
	Keen        *bool    `parquet:"keen"`
	Birthday    uint32   `parquet:"birthday"`
	Anniversary *uint64  `parquet:"anniversary"`
	Secret      string   `parquet:"-"`
	Sleepy      bool
}

func TestParquet(t *testing.T) {
	type testCase struct {
		name  string
		input []Person
		//if expected is nil then input is used for the assertions
		expected []Person
	}

	testCases := []testCase{
		{
			name: "single person",
			input: []Person{
				{Being: Being{ID: 1, Age: pint32(0)}},
			},
		},
		{
			name: "multiple people",
			input: []Person{
				{Being: Being{ID: 1, Age: pint32(-10)}},
				{Happiness: 55},
				{Sadness: pint64(1)},
				{Code: pstring("10!01")},
				{Funkiness: 0.2},
				{Lameness: pfloat32(-0.4)},
				{Keen: pbool(true)},
				{Birthday: 55},
				{Anniversary: puint64(1010010)},
				{Secret: "hush hush"},
				{Keen: pbool(false)},
				{Sleepy: true},
			},
			expected: []Person{
				{Being: Being{ID: 1, Age: pint32(-10)}},
				{Happiness: 55},
				{Sadness: pint64(1)},
				{Code: pstring("10!01")},
				{Funkiness: 0.2},
				{Lameness: pfloat32(-0.4)},
				{Keen: pbool(true)},
				{Birthday: 55},
				{Anniversary: puint64(1010010)},
				{Secret: ""},
				{Keen: pbool(false)},
				{Sleepy: true},
			},
		},
		{
			name: "boolean optional",
			input: []Person{
				{Keen: nil},
				{Keen: pbool(true)},
				{Keen: nil},
				{Keen: pbool(false)},
				{Keen: nil},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewParquetWriter(&buf)
			if !assert.Nil(t, err, tc.name) {
				return
			}
			for _, p := range tc.input {
				w.Add(p)
			}
			err = w.Write()
			if !assert.Nil(t, err, tc.name) {
				return
			}
			err = w.Close()
			if !assert.Nil(t, err, tc.name) {
				return
			}

			r, err := NewParquetReader(bytes.NewReader(buf.Bytes()))
			if !assert.Nil(t, err) {
				return
			}

			expected := tc.expected
			if expected == nil {
				expected = tc.input
			}

			var i int
			for r.Next() {
				var p Person
				r.Scan(&p)
				assert.Equal(t, expected[i], p, fmt.Sprintf("%s-%d", tc.name, i))
				i++
			}

			assert.Nil(t, r.Error(), tc.name)
			assert.Equal(t, i, len(expected), tc.name)
		})
	}
}

func pint32(i int32) *int32       { return &i }
func puint32(i uint32) *uint32    { return &i }
func pint64(i int64) *int64       { return &i }
func puint64(i uint64) *uint64    { return &i }
func pbool(b bool) *bool          { return &b }
func pstring(s string) *string    { return &s }
func pfloat32(f float32) *float32 { return &f }
func pfloat64(f float64) *float64 { return &f }
