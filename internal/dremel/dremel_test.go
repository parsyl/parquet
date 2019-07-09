package dremel_test

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

//go:generate parquetgen -input dremel_test.go -type Document -package dremel_test -output dremel_generated_test.go

func TestDremel(t *testing.T) {
	docs := []Document{
		{
			DocID: 10,
			Links: &Link{Forward: []int64{20, 40, 60}},
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
			Links: &Link{Backward: []int64{10, 30}, Forward: []int64{80}},
			Names: []Name{{URL: pstring("http://C")}},
		},
	}

	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf)
	if err != nil {
		log.Fatal(err)
	}

	for _, doc := range docs {
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

	expected := []Levels{
		{Name: "docid"},
		{Name: "backward", Defs: []uint8{1, 2, 2}, Reps: []uint8{0, 0, 1}},
		{Name: "forward", Defs: []uint8{2, 2, 2, 2}, Reps: []uint8{0, 1, 1, 0}},
		{Name: "code", Defs: []uint8{2, 2, 1, 2, 1}, Reps: []uint8{0, 2, 1, 1, 0}},
		{Name: "country", Defs: []uint8{3, 2, 1, 3, 1}, Reps: []uint8{0, 2, 1, 1, 0}},
		{Name: "url", Defs: []uint8{2, 2, 1, 2}, Reps: []uint8{0, 1, 1, 0}},
	}

	assert.Equal(t, expected, pr.Levels())
}

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
	Links *Link
	Names []Name
}

func pstring(s string) *string {
	return &s
}
