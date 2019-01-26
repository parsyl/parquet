package main

import (
	"io"

	"github.com/parsyl/parquet"
)

func NewParquetReader(r io.ReadSeeker) (*ParquetReader, error) {
	ff := Fields()
	schema := make([]parquet.Field, len(ff))
	for i, f := range ff {
		schema[i] = f.Schema()
	}

	pr := &ParquetReader{meta: parquet.New(schema...), r: r}

	if err := pr.meta.Read(pr.r); err != nil {
		return nil, err
	}

	pr.rows = pr.meta.Rows()

	err := pr.meta.ReadChunks(0)
	return pr, err

}

type ParquetReader struct {
	r    io.ReadSeeker
	meta *parquet.Metadata
	err  error
	cur  int
	rows int
}

func (p *ParquetReader) Error() error {
	return p.err
}

func (p *ParquetReader) Next() bool {
	if p.cur >= p.rows || p.err != nil {
		return false
	}
}

func (p *ParquetReader) Read(x *Person) error {
	return nil
}
