package main

var boolTpl = `{{define "boolField"}}type BoolField struct {
	parquet.RequiredField
	vals []bool
	val  func(r {{.Type}}) bool
	read func(r *{{.Type}}, v bool)
}

func NewBoolField(val func(r {{.Type}}) bool, read func(r *{{.Type}}, v bool), col string) *BoolField {
	return &BoolField{
		val:           val,
		read:          read,
		RequiredField: parquet.NewRequiredField(col),
	}
}

func (f *BoolField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.BoolType, RepetitionType: parquet.RepetitionRequired}
}

func (f *BoolField) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}

	v := f.vals[0]
	f.vals = f.vals[1:]
	f.read(r, v)
}

func (f *BoolField) Add(r {{.Type}}) {
	f.vals = append(f.vals, f.val(r))
}

func (f *BoolField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals))
}

func (f *BoolField) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, sizes, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	f.vals, err = parquet.GetBools(rr, int(pos.N), sizes)
	return err
}
{{end}}`
