package parquet_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/parsyl/parquet"
	sch "github.com/parsyl/parquet/schema"
	"github.com/stretchr/testify/assert"
)

//go:generate parquetgen -input parquet_test.go -type Person -package parquet_test -output parquet_generated_test.go

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

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
			name: "single nested person",
			input: [][]Person{
				{{Hobby: &Hobby{Name: "napping", Difficulty: pint32(10)}}},
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
					{Hobby: &Hobby{Name: "napping", Difficulty: pint32(10)}},
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
					{Hobby: &Hobby{Name: "napping", Difficulty: pint32(10)}},
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
			name:  "lots of people",
			input: getPeople(100, 5000),
		},
		{
			name:     "lots of people small page size",
			pageSize: 5,
			input:    getPeople(100, 5000),
		},
		{
			name:     "lots of people small row group size",
			pageSize: 100,
			input:    getPeople(5, 5000),
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
			name:     "boolean optional lots of repetition",
			pageSize: 5,
			input: [][]Person{
				{
					{Keen: pbool(true)},
					{Keen: pbool(true)},
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: nil},
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
		{
			name:     "repeated two pages",
			pageSize: 2,
			input: [][]Person{
				{
					{
						Friends: []Being{
							{ID: 1, Age: pint32(10)},
						},
					},
					{Code: pstring("c")},
					{
						Friends: []Being{
							{ID: 2, Age: pint32(12)},
							{ID: 3, Age: pint32(14)},
						},
					},
				},
				{
					{Code: pstring("g")},
					{
						Friends: []Being{
							{ID: 4, Age: pint32(16)},
							{ID: 5, Age: pint32(18)},
						},
					},
					{
						Friends: []Being{
							{ID: 6, Age: pint32(20)},
							{ID: 7, Age: pint32(22)},
						},
					},
				},
			},
		},
	}

	for i, tc := range testCases {
		for j, comp := range []string{"uncompressed", "snappy", "gzip"} {
			t.Run(fmt.Sprintf("%02d %s %s", 2*i+j, tc.name, comp), func(t *testing.T) {
				if tc.pageSize == 0 {
					tc.pageSize = 100
				}
				var buf bytes.Buffer
				w, err := NewParquetWriter(&buf, MaxPageSize(tc.pageSize), compressionTest[comp])
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
				if !assert.NoError(t, err) {
					return
				}

				expected := tc.expected
				if expected == nil {
					expected = tc.input
				}

				if !assert.Equal(t, getLen(expected), int(r.Rows()), tc.name) {
					return
				}

				var i int
				for r.Next() {
					var p Person
					r.Scan(&p)
					exp := getExpected(expected, i)
					assert.Equal(t, *exp, p, fmt.Sprintf("%s-%d", tc.name, i))
					i++
				}

				assert.Nil(t, r.Error(), tc.name)
				assert.Equal(t, getLen(expected), i, tc.name)
			})
		}
	}
}

func TestPageHeaders(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewParquetWriter(&buf, MaxPageSize(2))
	if !assert.NoError(t, err) {
		return
	}

	docs := [][]Person{
		{{}, {}, {}, {}},
		{{}, {}, {}, {}},
	}
	for _, rowgroup := range docs {
		for _, p := range rowgroup {
			w.Add(p)
		}
		assert.NoError(t, w.Write())
	}

	assert.NoError(t, w.Close())

	rd := bytes.NewReader(buf.Bytes())
	footer, err := parquet.ReadMetaData(rd)
	if !assert.NoError(t, err) {
		return
	}

	pageHeaders, err := parquet.PageHeaders(footer, rd)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, 72, len(pageHeaders))
}

func TestStats(t *testing.T) {
	type stats struct {
		min      []byte
		max      []byte
		nilCount *int64
	}

	type testCase struct {
		name     string
		input    [][]Person
		pageSize int
		stats    []stats
		col      string
	}

	testCases := []testCase{
		{
			name: "int64 single stat",
			col:  "happiness",
			input: [][]Person{
				{
					{Happiness: 1},
					{Happiness: 2},
					{Happiness: 22},
				},
			},
			stats: []stats{
				{min: writeInt64(1), max: writeInt64(22)},
			},
		},
		{
			name:     "int64 two pages",
			col:      "happiness",
			pageSize: 2,
			input: [][]Person{
				{
					{Happiness: 1},
					{Happiness: 2},
					{Happiness: 22},
				},
			},
			stats: []stats{
				{min: writeInt64(1), max: writeInt64(2)},
				{min: writeInt64(22), max: writeInt64(22)},
			},
		},
		{
			name: "int64 no stats",
			col:  "happiness",
			input: [][]Person{
				{
					{Being: Being{ID: 1}},
					{Being: Being{ID: 2}},
					{Being: Being{ID: 3}},
				},
			},
			stats: []stats{
				{min: writeInt64(0), max: writeInt64(0)},
			},
		},
		{
			name: "optional int64 no stats",
			col:  "sadness",
			input: [][]Person{
				{
					{Being: Being{ID: 1}},
					{Being: Being{ID: 2}},
					{Being: Being{ID: 3}},
				},
			},
			stats: []stats{
				{min: nil, max: nil, nilCount: pint64(3)},
			},
		},
		{
			name: "int32 stats",
			col:  "birthday",
			input: [][]Person{
				{
					{Birthday: 10},
					{Birthday: 20},
					{Birthday: 30},
				},
			},
			stats: []stats{
				{min: writeInt32(10), max: writeInt32(30)},
			},
		},
		{
			name: "float64 stats",
			col:  "boldness",
			input: [][]Person{
				{
					{Boldness: 0.5},
					{Boldness: 500.0},
					{Boldness: -50.5},
				},
			},
			stats: []stats{
				{min: writeFloat64(-50.5), max: writeFloat64(500.0)},
			},
		},
		{
			name: "float32 optional stats",
			col:  "lameness",
			input: [][]Person{
				{
					{Lameness: pfloat32(0.5)},
					{Lameness: pfloat32(500.0)},
					{Lameness: pfloat32(50.5)},
					{Lameness: nil},
				},
			},
			stats: []stats{
				{min: writeFloat32(0.5), max: writeFloat32(500.0), nilCount: pint64(1)},
			},
		},
		{
			name: "bool stats",
			col:  "hungry",
			input: [][]Person{
				{
					{Hungry: true},
					{Hungry: false},
					{Hungry: true},
				},
			},
			stats: []stats{
				{},
			},
		},
		{
			name: "optional bool stats",
			col:  "keen",
			input: [][]Person{
				{
					{Keen: pbool(true)},
					{Keen: nil},
					{Keen: nil},
				},
			},
			stats: []stats{
				{nilCount: pint64(2)},
			},
		},
		{
			name: "string stats",
			col:  "bff",
			input: [][]Person{
				{
					{BFF: "Fred"},
					{BFF: "Val"},
					{BFF: "Miranda"},
				},
			},
			stats: []stats{
				{min: []byte("Fred"), max: []byte("Val")},
			},
		},
		{
			name: "string optional stats",
			col:  "code",
			input: [][]Person{
				{
					{Code: pstring("Fred")},
					{Code: nil},
					{Code: pstring("Miranda")},
				},
			},
			stats: []stats{
				{min: []byte("Fred"), max: []byte("Miranda"), nilCount: pint64(1)},
			},
		},
	}

	for i, tc := range testCases {
		for j, comp := range []string{"uncompressed", "snappy"} {
			t.Run(fmt.Sprintf("%02d %s %s", 2*i+j, tc.name, comp), func(t *testing.T) {
				if tc.pageSize == 0 {
					tc.pageSize = 100
				}
				var buf bytes.Buffer
				w, err := NewParquetWriter(&buf, MaxPageSize(tc.pageSize), compressionTest[comp])
				assert.Nil(t, err, tc.name)
				for _, rowgroup := range tc.input {
					for _, p := range rowgroup {
						w.Add(p)
					}
					assert.Nil(t, w.Write(), tc.name)
				}

				err = w.Close()
				assert.Nil(t, err, tc.name)

				r := bytes.NewReader(buf.Bytes())
				footer, err := parquet.ReadMetaData(r)
				if !assert.NoError(t, err) {
					return
				}

				pages, err := getPageHeaders(r, tc.col, footer)
				if !assert.NoError(t, err) {
					return
				}

				if !assert.Equal(t, len(tc.stats), len(pages), tc.name) {
					return
				}
				for i, st := range tc.stats {
					ph := pages[i]
					assert.Equal(t, st.min, ph.DataPageHeader.Statistics.MinValue)
					assert.Equal(t, st.max, ph.DataPageHeader.Statistics.MaxValue)
					if st.nilCount == nil {
						assert.Equal(t, st.nilCount, ph.DataPageHeader.Statistics.NullCount)
					} else {
						assert.Equal(t, *st.nilCount, *ph.DataPageHeader.Statistics.NullCount)
					}
				}
			})
		}
	}
}

func getPageHeaders(r io.ReadSeeker, name string, footer *sch.FileMetaData) ([]sch.PageHeader, error) {
	var out []sch.PageHeader
	for _, rg := range footer.RowGroups {
		for _, col := range rg.Columns {
			pth := col.MetaData.PathInSchema
			if pth[len(pth)-1] == name {
				h, err := parquet.PageHeadersAtOffset(r, col.MetaData.DataPageOffset, col.MetaData.NumValues)
				if err != nil {
					return nil, err
				}
				out = append(out, h...)
			}
		}
	}
	return out, nil
}

var compressionTest = map[string]func(*ParquetWriter) error{
	"uncompressed": Uncompressed,
	"snappy":       Snappy,
	"gzip":         Gzip,
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
		rg = append(rg, Person{Sleepy: rand.Intn(2) == 1})
	}
	if len(rg) > 0 {
		out = append(out, rg)
	}
	return out
}

func randString(n int) *string {
	if rand.Intn(2) == 0 {
		return nil
	}

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	s := string(b)
	return &s
}

func getPeople(rgSize, n int) [][]Person {
	var out [][]Person
	var rg []Person
	for i := 0; i < n; i++ {
		if i > 0 && i%rgSize == 0 {
			out = append(out, rg)
			rg = []Person{}
		}
		rg = append(rg, newPerson(i))
	}

	if len(rg) > 0 {
		out = append(out, rg)
	}
	return out
}

func newPerson(i int) Person {
	var age *int32
	if i%2 == 0 {
		a := int32(20 + i%5)
		age = &a
	}

	var sadness *int64
	if i%3 == 0 {
		s := int64(i + 5)
		sadness = &s
	}

	var lameness *float32
	if rand.Intn(2) == 0 {
		l := rand.Float32()
		lameness = &l
	}

	var keen *bool
	if i%5 == 0 {
		b := true
		keen = &b
	}

	var anv *uint64
	if i%3 == 0 {
		x := math.MaxUint64 - uint64(i*100)
		anv = &x
	}

	return Person{
		Being: Being{
			ID:  int32(i),
			Age: age,
		},
		Happiness:   int64(i * 2),
		Sadness:     sadness,
		Code:        randString(8),
		Funkiness:   rand.Float32(),
		Boldness:    rand.Float64(),
		Lameness:    lameness,
		Keen:        keen,
		Birthday:    uint32(i * 1000),
		Anniversary: anv,
	}
}

func BenchmarkRead(b *testing.B) {
	var buf bytes.Buffer
	w, err := NewParquetWriter(&buf, MaxPageSize(10000))
	assert.Nil(b, err, "benchmark read")
	input := getPeople(100000, b.N)
	for _, rowgroup := range input {
		for _, p := range rowgroup {
			w.Add(p)
		}
		assert.Nil(b, w.Write(), "benchmark read")
	}

	err = w.Close()
	assert.Nil(b, err, "benchmark read")

	r, err := NewParquetReader(bytes.NewReader(buf.Bytes()))
	assert.Nil(b, err)

	for i := 0; i < b.N; i++ {
		if !r.Next() {
			b.Fatal("unexpected end of Next()")
		}
		var p Person
		r.Scan(&p)
	}
}

func BenchmarkWrite(b *testing.B) {
	var buf bytes.Buffer
	w, err := NewParquetWriter(&buf, MaxPageSize(10000))
	assert.Nil(b, err, "benchmark write")
	input := getPeople(b.N, b.N)
	rg := input[0]

	for i := 0; i < b.N; i++ {
		p := rg[i]
		w.Add(p)
	}

	err = w.Close()
	assert.Nil(b, err, "benchmark write")
}

func writeInt64(i int64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, i)
	return buf.Bytes()
}

func writeInt32(i int32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, i)
	return buf.Bytes()
}

func writeFloat32(f float32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, f)
	return buf.Bytes()
}

func writeFloat64(f float64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, f)
	return buf.Bytes()
}

type Being struct {
	ID  int32  `parquet:"id"`
	Age *int32 `parquet:"age"`
}

type Hobby struct {
	Name       string `parquet:"name"`
	Difficulty *int32 `parquet:"difficulty"`
}

type Person struct {
	Being
	Happiness   int64    `parquet:"happiness"`
	Sadness     *int64   `parquet:"sadness"`
	Code        *string  `parquet:"code"`
	Funkiness   float32  `parquet:"funkiness"`
	Boldness    float64  `parquet:"boldness"`
	Lameness    *float32 `parquet:"lameness"`
	Keen        *bool    `parquet:"keen"`
	Birthday    uint32   `parquet:"birthday"`
	Anniversary *uint64  `parquet:"anniversary"`
	BFF         string   `parquet:"bff"`
	Hungry      bool     `parquet:"hungry"`
	Secret      string   `parquet:"-"`
	Hobby       *Hobby   `parquet:"hobby"`
	Friends     []Being  `parquet:"friends"`
	Sleepy      bool
}

/*
type Name struct {
}

message Document {
  required int64 DocId;
  optional group Links {
    repeated int64 Backward;
    repeated int64 Forward; }
  repeated group Name {
    repeated group Language {
      required string Code;
      optional string Country; }
    optional string Url; }}
*/

type Link struct {
	Backward []int64
	Forward  []int64
}

type Language struct {
	Code    string
	Country *string
}

type Name struct {
	Languages []Language
	URL       *string
}

type Document struct {
	DocID int64
	Links []Link
	Names []Name
}
