package dremel_test

import (
	"bytes"
	"testing"

	"github.com/parsyl/parquet/cmd/parquetgen/dremel/testcases/doc"
	"github.com/parsyl/parquet/cmd/parquetgen/dremel/testcases/person"
	"github.com/parsyl/parquet/cmd/parquetgen/dremel/testcases/repetition"
	"github.com/stretchr/testify/assert"
)

var (
	dremelDocs = []doc.Document{
		{
			DocID: 10,
			Links: &doc.Link{
				Forward: []int64{20, 40, 60},
			},
			Names: []doc.Name{
				{
					Languages: []doc.Language{
						{Code: "en-us", Country: pstring("us")},
						{Code: "en"},
					},
					URL: pstring("http://A"),
				},
				{
					URL: pstring("http://B"),
				},
				{
					Languages: []doc.Language{
						{Code: "en-gb", Country: pstring("gb")},
					},
				},
			},
		},
		{
			DocID: 20,
			Links: &doc.Link{
				Backward: []int64{10, 30},
				Forward:  []int64{80},
			},
			Names: []doc.Name{
				{
					URL: pstring("http://C"),
				},
			},
		},
	}
)

// TestLevels verifies that the example from the dremel paper
// results in the correct definition and repetition levels.
func TestLevels(t *testing.T) {
	var buf bytes.Buffer
	pw, err := doc.NewParquetWriter(&buf)
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

	pr, err := doc.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		assert.NoError(t, err)
	}

	expected := []doc.Levels{
		{Name: "docid"},
		{Name: "link.backward", Defs: []uint8{1, 2, 2}, Reps: []uint8{0, 0, 1}},
		{Name: "link.forward", Defs: []uint8{2, 2, 2, 2}, Reps: []uint8{0, 1, 1, 0}},
		{Name: "names.languages.code", Defs: []uint8{2, 2, 1, 2, 1}, Reps: []uint8{0, 2, 1, 1, 0}},
		{Name: "names.languages.country", Defs: []uint8{3, 2, 1, 3, 1}, Reps: []uint8{0, 2, 1, 1, 0}},
		{Name: "names.url", Defs: []uint8{2, 2, 1, 2}, Reps: []uint8{0, 1, 1, 0}},
	}

	assert.Equal(t, expected, pr.Levels())
}

var (
	people = []person.Person{
		{
			Name: "peep",
			Hobby: &person.Hobby{
				Name:       "napping",
				Difficulty: pint32(10),
				Skills: []person.Skill{
					{Name: "meditation", Difficulty: "very"},
					{Name: "calmness", Difficulty: "so-so"},
				},
			},
		},
	}
)

func TestPersonLevels(t *testing.T) {
	var buf bytes.Buffer
	pw, err := person.NewParquetWriter(&buf)
	if err != nil {
		assert.NoError(t, err)
	}

	for _, p := range people {
		pw.Add(p)
	}

	if err := pw.Write(); err != nil {
		assert.NoError(t, err)
	}

	pw.Close()

	pr, err := person.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		assert.NoError(t, err)
	}

	expected := []person.Levels{
		{Name: "name"},
		{Name: "hobby.name", Defs: []uint8{1}},
		{Name: "hobby.difficulty", Defs: []uint8{2}},
		{Name: "hobby.skills.name", Defs: []uint8{2, 2}, Reps: []uint8{0, 1}},
		{Name: "hobby.skills.difficulty", Defs: []uint8{2, 2}, Reps: []uint8{0, 1}},
	}

	assert.Equal(t, expected, pr.Levels())
}

// TestDremel uses the example from the dremel paper and writes then
// reads from a parquet file to make sure nested fields work correctly.
func TestDremel(t *testing.T) {
	var buf bytes.Buffer
	pw, err := doc.NewParquetWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}

	for _, doc := range dremelDocs {
		pw.Add(doc)
	}

	if err := pw.Write(); err != nil {
		t.Fatal(err)
	}

	pw.Close()

	pr, err := doc.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	var out []doc.Document
	for pr.Next() {
		var d doc.Document
		pr.Scan(&d)
		out = append(out, d)
	}

	assert.Equal(t, dremelDocs, out)
}

func pstring(s string) *string {
	return &s
}

func pint32(i int32) *int32 {
	return &i
}

var (
	repetitionDocs = []repetition.Document{
		{
			Links: []repetition.Link{
				{
					Backward: []repetition.Language{{Codes: []string{"a", "b"}}},
					Forward:  []repetition.Language{{Codes: []string{"aa", "bbb"}}},
				},
				{
					Backward: nil,
					Forward:  []repetition.Language{{Codes: []string{"c", "d"}}},
				},
				{
					Backward: []repetition.Language{{Countries: []string{"e", "f"}}},
					Forward:  nil,
				},
				{
					Backward: nil,
					Forward:  []repetition.Language{{Countries: []string{"g", "h"}}},
				},
				{
					Backward: []repetition.Language{{Countries: []string{"i", "j"}}},
					Forward:  []repetition.Language{{Codes: []string{"k", "l"}}},
				},
				{
					Backward: []repetition.Language{
						{
							Codes:     []string{"m", "n"},
							Countries: []string{"o", "p"},
						},
						{
							Codes:     []string{"q", "r"},
							Countries: []string{"s", "t"},
						},
					},
					Forward: []repetition.Language{{Countries: []string{"u", "v"}}},
				},
				{
					Backward: []repetition.Language{{Codes: []string{"w", "x"}}},
					Forward:  []repetition.Language{{Countries: []string{"y", "z"}}},
				},
				{
					Backward: []repetition.Language{
						{
							Codes:     []string{"aa"},
							URL:       pstring("http://abc.com"),
							Countries: []string{"ab"},
						},
						{
							URL:       pstring("http://abc.com"),
							Countries: []string{"ac"},
						},
						{
							Codes: []string{"ad"},
							URL:   pstring("http://abc.com"),
						},
					},
					Forward: []repetition.Language{
						{
							Countries: []string{"y", "z"},
							URL:       pstring("http://abc.com"),
						},
					},
				},
			},
		},
	}
)

func TestRepetition(t *testing.T) {
	var buf bytes.Buffer
	pw, err := repetition.NewParquetWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}

	for _, doc := range repetitionDocs {
		pw.Add(doc)
	}

	if err := pw.Write(); err != nil {
		t.Fatal(err)
	}

	pw.Close()

	pr, err := repetition.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	var out []repetition.Document
	for pr.Next() {
		var d repetition.Document
		pr.Scan(&d)
		out = append(out, d)
	}

	assert.Equal(t, repetitionDocs, out)
}
