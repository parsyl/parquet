package parquet_test

import (
	"bytes"
	"fmt"
	"math/rand"
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
		input [][]Person
		//if expected is nil then input is used for the assertions
		expected [][]Person
		pageSize int
	}

	testCases := []testCase{
		{
			name: "single person",
			input: [][]Person{
				{{Being: Being{ID: 1, Age: pint32(0)}}},
			},
		},
		{
			name: "multiple people",
			input: [][]Person{
				{
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
			},
			expected: [][]Person{
				{
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
		},
		{
			name:     "multiple people multiple row groups small page size",
			pageSize: 2,
			input: [][]Person{
				{
					{Being: Being{ID: 1, Age: pint32(-10)}},
					{Happiness: 55},
					{Sadness: pint64(1)},
					{Code: pstring("10!01")},
					{Funkiness: 0.2},
				},
				{
					{Lameness: pfloat32(-0.4)},
					{Keen: pbool(true)},
					{Birthday: 55},
					{Anniversary: puint64(1010010)},
					{Secret: "hush hush"},
					{Keen: pbool(false)},
					{Sleepy: true},
				},
			},
			expected: [][]Person{
				{
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
		},
		{
			name: "numeric optional",
			input: [][]Person{
				{
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(1)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(-2)}},
					{Being: Being{Age: nil}},
				},
			},
		},
		{
			name:     "numeric optional small page size",
			pageSize: 2,
			input: [][]Person{
				{
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(3)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(-4)}},
					{Being: Being{Age: nil}},
				},
			},
		},
		{
			name: "numeric optional multiple row groups",
			input: [][]Person{
				{
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(5)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(-6)}},
					{Being: Being{Age: nil}},
				},
				{
					{Being: Being{Age: pint32(7)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(-8)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(9)}},
				},
			},
		},
		{
			name:     "numeric optional multiple row groups small page size",
			pageSize: 3,
			input: [][]Person{
				{
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(5)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(-6)}},
					{Being: Being{Age: nil}},
				},
				{
					{Being: Being{Age: pint32(7)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(-8)}},
					{Being: Being{Age: nil}},
					{Being: Being{Age: pint32(9)}},
				},
			},
		},
		{
			name: "boolean optional",
			input: [][]Person{
				{
					{Keen: nil},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
				},
			},
		},
		{
			name:     "boolean optional small page size",
			pageSize: 2,
			input: [][]Person{
				{
					{Keen: nil},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
				},
			},
		},
		{
			name: "boolean optional multiple row groups",
			input: [][]Person{
				{
					{Keen: nil},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
				},
				{
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
					{Keen: pbool(true)},
				},
			},
		},
		{
			name:     "boolean optional multiple row groups small page size",
			pageSize: 2,
			input: [][]Person{
				{
					{Keen: nil},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
				},
				{
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
					{Keen: pbool(true)},
				},
			},
		},
		{
			name:     "boolean optional more than eight in a page",
			pageSize: 2,
			input: [][]Person{
				{
					{Keen: nil},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: pbool(false)},
					{Keen: nil},
					{Keen: pbool(true)},
				},
			},
		},
		{
			name:     "boolean optional large amount small page size",
			pageSize: 2,
			input:    getOptBools(31),
		},
		{
			name:     "boolean optional really large amount small page size",
			pageSize: 2,
			input:    getOptBools(3001),
		},
		{
			name:     "boolean optional really large amount large page size",
			pageSize: 3000,
			input:    getOptBools(3001),
		},
		{
			name:     "boolean really large amount large page size",
			pageSize: 3000,
			input:    getBools(3001),
		},
		{
			name:     "boolean really large amount small page size",
			pageSize: 3,
			input:    getBools(3001),
		},
		{
			name:     "boolean multiple row groups small page size",
			pageSize: 2,
			input: [][]Person{
				{
					{Sleepy: false},
					{Sleepy: true},
					{Sleepy: true},
					{Sleepy: false},
					{Sleepy: true},
				},
				{
					{Sleepy: true},
					{Sleepy: true},
					{Sleepy: false},
					{Sleepy: true},
					{Sleepy: true},
				},
			},
		},
		{
			name: "boolean more than eight in a page",
			input: [][]Person{
				{
					{Sleepy: false},
					{Sleepy: true},
					{Sleepy: true},
					{Sleepy: false},
					{Sleepy: true},
					{Sleepy: true},
					{Sleepy: true},
					{Sleepy: false},
					{Sleepy: true},
					{Sleepy: true},
				},
			},
		},
		{
			name:     "optional string multiple row groups small page size",
			pageSize: 2,
			input: [][]Person{
				{
					{Code: pstring("a")},
					{Code: pstring("b")},
					{Code: pstring("c")},
					{Code: pstring("d")},
					{Code: pstring("e")},
				},
				{
					{Code: pstring("f")},
					{Code: pstring("g")},
					{Code: pstring("h")},
					{Code: pstring("i")},
					{Code: pstring("j")},
				},
			},
		},
		{
			name:     "optional string multiple row groups small page size with nil values",
			pageSize: 2,
			input: [][]Person{
				{
					{Code: pstring("a")},
					{Code: pstring("b")},
					{Code: nil},
					{Code: pstring("c")},
					{Code: nil},
				},
				{
					{Code: nil},
					{Code: pstring("d")},
					{Code: pstring("e")},
					{Code: pstring("f")},
					{Code: nil},
					{Code: pstring("g")},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.pageSize == 0 {
				tc.pageSize = 100
			}
			var buf bytes.Buffer
			w, err := NewParquetWriter(&buf, MaxPageSize(tc.pageSize))
			assert.Nil(t, err, tc.name)
			for _, rowgroup := range tc.input {
				for _, p := range rowgroup {
					w.Add(p)
				}
				assert.Nil(t, w.Write(), tc.name)
			}

			err = w.Close()
			assert.Nil(t, err, tc.name)

			r, err := NewParquetReader(bytes.NewReader(buf.Bytes()))
			assert.Nil(t, err)
			expected := tc.expected
			if expected == nil {
				expected = tc.input
			}

			assert.Equal(t, getLen(expected), int(r.Rows()), tc.name)

			var i int
			for r.Next() {
				var p Person
				r.Scan(&p)
				exp := getExpected(expected, i)
				assert.Equal(t, *exp, p, fmt.Sprintf("%s-%d", tc.name, i))
				i++
			}

			assert.Nil(t, r.Error(), tc.name)
			assert.Equal(t, i, getLen(expected), tc.name)
		})
	}
}

func getLen(peeps [][]Person) int {
	var l int
	for _, rg := range peeps {
		l += len(rg)
	}
	return l
}

func getExpected(peeps [][]Person, i int) *Person {
	for _, rg := range peeps {
		if i < len(rg) {
			return &rg[i]
		}
		i -= len(rg)
	}
	return nil
}

func pint32(i int32) *int32       { return &i }
func puint32(i uint32) *uint32    { return &i }
func pint64(i int64) *int64       { return &i }
func puint64(i uint64) *uint64    { return &i }
func pbool(b bool) *bool          { return &b }
func pstring(s string) *string    { return &s }
func pfloat32(f float32) *float32 { return &f }
func pfloat64(f float64) *float64 { return &f }

func getOptBools(count int) [][]Person {
	var out [][]Person
	var rg []Person
	for i := 0; i < count; i++ {
		if i > 0 && i%100 == 0 {
			out = append(out, rg)
			rg = []Person{}
		}
		r := rand.Intn(3)
		var b *bool
		switch r {
		case 0:
			x := true
			b = &x
		case 1:
			x := false
			b = &x
		}
		rg = append(rg, Person{Keen: b})
	}
	if len(rg) > 0 {
		out = append(out, rg)
	}
	return out
}

func getBools(count int) [][]Person {
	var out [][]Person
	var rg []Person
	for i := 0; i < count; i++ {
		if i > 0 && i%100 == 0 {
			out = append(out, rg)
			rg = []Person{}
		}
		r := rand.Intn(2)
		var b bool
		switch r {
		case 0:
			b = true
		}
		rg = append(rg, Person{Sleepy: b})
	}
	if len(rg) > 0 {
		out = append(out, rg)
	}
	return out
}
