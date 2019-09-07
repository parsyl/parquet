package dremel_test

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

//go:generate parquetgen -input dremel_test.go -type Document -package dremel_test -output dremel_generated_test.go

var (
	dremelDocs = []Document{
		{
			DocID: 10,
			Link:  &Link{Forward: []int64{20, 40, 60}},
			Names: []Name{
				{
					Languages: []Language{
						{Code: "en-us", Country: pstring("us")},
						{Code: "en"},
					},
					URL: pstring("http://A"),
				},
				{
					URL: pstring("http://B"),
				},
				{
					Languages: []Language{
						{Code: "en-gb", Country: pstring("gb")},
					},
				},
			},
		},
		{
			DocID: 20,
			Link:  &Link{Backward: []int64{10, 30}, Forward: []int64{80}},
			Names: []Name{{URL: pstring("http://C")}},
		},
	}
)

// TestLevels verifies that the example from the dremel paper
// results in the correct definition and repetition levels.
func TestLevels(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf)
	if err != nil {
		assert.NoError(t, err)
	}

	for _, doc := range dremelDocs {
		pw.Add(doc)
	}

	if err := pw.Write(); err != nil {
		assert.NoError(t, err)
	}

	pw.Close()

	pr, err := NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		assert.NoError(t, err)
	}

	expected := []Levels{
		{Name: "docid"},
		{Name: "link.backward", Defs: []uint8{1, 2, 2}, Reps: []uint8{0, 0, 1}},
		{Name: "link.forward", Defs: []uint8{2, 2, 2, 2}, Reps: []uint8{0, 1, 1, 0}},
		{Name: "names.languages.code", Defs: []uint8{2, 2, 1, 2, 1}, Reps: []uint8{0, 2, 1, 1, 0}},
		{Name: "names.languages.country", Defs: []uint8{3, 2, 1, 3, 1}, Reps: []uint8{0, 2, 1, 1, 0}},
		{Name: "names.url", Defs: []uint8{2, 2, 1, 2}, Reps: []uint8{0, 1, 1, 0}},
	}

	assert.Equal(t, expected, pr.Levels())
}

// TestDremel uses the example from the dremel paper and writes then
// reads from a parquet file to make sure nested fields work correctly.
func TestDremel(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf)
	if err != nil {
		log.Fatal(err)
	}

	for _, doc := range dremelDocs {
		pw.Add(doc)
	}

	if err := pw.Write(); err != nil {
		log.Fatal(err)
	}

	pw.Close()

	pr, err := NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Fatal(err)
	}

	var out []Document
	for pr.Next() {
		var d Document
		pr.Scan(&d)
		out = append(out, d)
	}

	assert.Equal(t, dremelDocs, out)
}

type Link struct {
	Backward []int64 `parquet:"backward"`
	Forward  []int64 `parquet:"forward"`
}

type Language struct {
	Code    string  `parquet:"code"`
	Country *string `parquet:"country"`
}

type Name struct {
	Languages []Language `parquet:"languages"`
	URL       *string    `parquet:"url"`
}

type Document struct {
	DocID int64  `parquet:"docid"`
	Link  *Link  `parquet:"link"`
	Names []Name `parquet:"names"`
}
