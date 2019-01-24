package parquet

import (
	"io"

	"github.com/cswank/parquet/schema"
)

// Records reprents a row group
type Records struct {
	fields []Field

	len int

	// records are for subsequent chunks
	records *Records

	// max is the number of Record items that can get written before
	// a new set of column chunks is written
	max int

	meta *schema.Metadata
	w    *writeCounter
}

func New(w io.Writer, fields []Field, meta *schema.Metadata, opts ...func(*Records)) *Records {
	r := &Records{
		max:    1000,
		w:      &writeCounter{w: w},
		fields: fields,
		meta:   meta,
	}

	for _, opt := range opts {
		opt(r)
	}
	return r
}

func Fields(f []Field) func(*Records) {
	return func(r *Records) {
		r.fields = f
	}
}

func Schema(f []schema.Field) func(*Records) {
	return func(r *Records) {
		r.meta = schema.New(f...)
	}
}

// MaxPageSize is the maximum number of rows in each row groups' page.
func MaxPageSize(m int) func(*Records) {
	return func(r *Records) {
		r.max = m
	}
}

func (r *Records) Write() error {
	if _, err := r.w.Write([]byte("PAR1")); err != nil {
		return err
	}

	for i, f := range r.fields {
		pos := r.w.n
		f.Write(r.w, r.meta, pos)

		for child := r.records; child != nil; child = child.records {
			pos := r.w.n
			child.fields[i].Write(r.w, r.meta, pos)
		}
	}

	if err := r.meta.Footer(r.w); err != nil {
		return err
	}

	_, err := r.w.Write([]byte("PAR1"))
	return err
}

func (r *Records) Add(rec Record) {
	if r.len == r.max {
		if r.records == nil {
			r.records = New(r.w, MaxPageSize(r.max))
			r.records.meta = r.meta
		}

		r.records.Add(rec)
		return
	}

	for _, f := range r.fields {
		f.Add(rec)
	}

	r.len++
}
