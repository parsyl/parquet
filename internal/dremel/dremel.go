package dremel

import (
	"github.com/parsyl/parquet/internal/fields"
)

// Package dremel generates code that parquetgen
// uses to encode/decode a struct for writing and
// reading parquet files.

func Write(i int, fields []fields.Field) string {
	f := fields[i]
	if !isOptional(f) && !isRepeated(f) {
		return writeRequired(f)
	}
	return writeOptional(i, fields)
}

func Read(f fields.Field) string {
	if isOptional(f) && !isRepeated(f) {
		return readOptional(f)
	}

	if isRepeated(f) {
		return readRepeated(f)
	}

	return readRequired(f)
}

func isRepeated(f fields.Field) bool {
	for _, o := range f.RepetitionTypes {
		if o == fields.Repeated {
			return true
		}
	}
	return false
}

func isOptional(f fields.Field) bool {
	for _, o := range f.RepetitionTypes {
		if o == fields.Optional {
			return true
		}
	}
	return false
}
