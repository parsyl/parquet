package dremel

import "github.com/parsyl/parquet/internal/parse"

func Write(f parse.Field) string {
	if !isOptional(f) {
		return writeRequired(f)
	}
	return writeNested(f)
}

func Read(f parse.Field) string {
	if len(f.Optionals) == 1 {
		return readFlat(f)
	}
	return readNested(f)
}

func isOptional(f parse.Field) bool {
	for _, o := range f.Optionals {
		if o {
			return true
		}
	}
	return false
}
