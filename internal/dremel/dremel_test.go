package dremel_test

import (
	"bytes"
	"testing"
)

//go:generate parquetgen -input dremel_test.go -type Document -package dremel_test -output dremel_generated_test.go

func TestDremel(t *testing.T) {
	docs := []Document{
		{
			DocID: 10,
			Links: &Link{{Forward: []int64{20, 40, 60}}},
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
			Links: &Link{{Backward: []int64{10, 30}, Forward: []int64{80}}},
			Names: []Name{{URL: pstring("http://C")}},
		},
	}

	var buf bytes.Buffer
	pw := NewParquetWriter(&buf)
	for _, doc := range docs {
		pw.Write(doc)
	}
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