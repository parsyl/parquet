package dremel

import (
	"github.com/parsyl/parquet/internal/fields"
)

// Package dremel generates code that parquetgen
// uses to encode/decode a struct for writing and
// reading parquet files.

func Write(i int, fields []fields.Field) string {
	f := fields[i]
	if f.Repeated() {
		return writeRepeated(i, fields)
	}
	if f.Optional() {
		return writeOptional(f)
	}

	return writeRequired(f)
}

func Read(f fields.Field) string {
	if f.Repeated() {
		return readRepeated(f)
	}

	if f.Optional() {
		return readOptional(f)
	}

	return readRequired(f)
}
