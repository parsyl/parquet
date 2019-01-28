package main

import (
	"io"

	"github.com/parsyl/parquet"
)

func NewParquetReader(r io.ReadSeeker, opts ...func(*ParquetReader)) (*ParquetReader, error) {
	ff := Fields()
	schema := make([]parquet.Field, len(ff))
	for i, f := range ff {
		schema[i] = f.Schema()
	}

	pr := &ParquetReader{
		fields: ff,
		meta:   parquet.New(schema...),
		r:      r,
	}

	for _, opt := range opts {
		opt(pr)
	}

	if pr.meta == nil {
		if err := pr.meta.Read(pr.r); err != nil {
			return nil, err
		}
		pr.rows = pr.meta.Rows()
		pr.offsets = pr.meta.Offsets()
	}

	offsets := pr.meta.Offsets()
	for _, f := range pr.fields {
		o := offsets[]
	}

	return pr, nil

}

func readerIndex(i int) func(*ParquetReader) {
	return func(p *ParquetReader) {
		p.index = i
	}
}

func readerMeta(m *parquet.Metadata) func(*ParquetReader) {
	return func(p *ParquetReader) {
		p.meta = m
	}
}

// ParquetReader reads one page from a row group.
type ParquetReader struct {
	fields []Field

	err error
	// index keeps track
	index   int
	cur     int
	rows    int
	offsets map[string][]parquet.Position
	// child points to the next page
	child *ParquetReader

	r    io.ReadSeeker
	meta *parquet.Metadata
}

func (p *ParquetReader) Error() error {
	return p.err
}

func (p *ParquetReader) Next() bool {
	if p.cur >= p.rows || p.err != nil {
		return false
	}
	return false
}

func (p *ParquetReader) Scan(x *Person) error {
	return nil
}
