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

	pr := &ParquetReader{
		fields: ff,
		meta:   parquet.New(schema...),
		r:      r,
	}

	if err := pr.meta.Read(pr.r); err != nil {
		return nil, err
	}

	pr.rows = pr.meta.Rows()
	pr.offsets = pr.meta.Offsets()

	// TODO: move this to Scan?
	// for i, o := range pr.Offsets {
	// 	for _,
	// }

	return pr, nil

}

type ParquetReader struct {
	fields []Field

	err     error
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
