package main

var boolTpl = `{{define "boolField"}}type BoolField struct {
	{{parquetType .}}
	vals []bool
	{{if isOptional .}}read   func(r {{.Type}}) (*bool, int64}}{{else}}read  func(r {{.Type}}) bool{{end}}
	write func(r *{{.Type}}, vals []bool, def int64) bool
}

func NewBoolField(read func(r {{.Type}}) bool, write func(r *{{.Type}}, v bool, def int64), col string, opts ...func(*{{parquetType .}})) *BoolField {
	return &BoolField{
		val:           val,
		read:          read,
		{{if isOptional .}}OptionalField: parquet.NewOptionalField(col, opts...),{{else}}RequiredField: parquet.NewRequiredField(col, opts...),{{end}}
	}
}

func (f *BoolField) Schema() parquet.Field {
	{{if isOptional .}}return parquet.Field{Name: f.Name(), Type: parquet.BoolType, RepetitionType: parquet.RepetitionOptional}{{else}}return parquet.Field{Name: f.Name(), Type: parquet.BoolType, RepetitionType: parquet.RepetitionRequired}{{end}}
}

func (f *BoolField) Scan(r *{{.Type}}) {
	{{ if isOptional .}}
	if len(f.vals) == 0 {
		return
	}

	v := f.vals[0]
	f.vals = f.vals[1:]
	f.write(r, v)
	{{else}}if len(f.Defs) == 0 {
		return
	}

	if f.write(r, f.vals, f.Defs[0]) {
		f.vals = f.vals[1:]
	}
	f.Defs = f.Defs[1:]{{end}}
}

func (f *BoolField) Add(r {{.Type}}) {
	{{if isOptional .}}v, def := f.read(r)
	f.stats.add(v)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.Defs = append(f.Defs, def)
	} else {
		f.Defs = append(f.Defs, 0)
	}{{else}}f.vals = append(f.vals, f.read(r)){{end}}
}

func (f *BoolField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	n := (ln + 7) / 8
	rawBuf := make([]byte, n)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals), newBoolStats())
}

func (f *BoolField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, sizes, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	f.vals, err = parquet.GetBools(rr, int(pg.N), sizes)
	return err
}
{{end}}`

var boolStatsTpl = `{{define "boolStats"}}
type boolStats struct {}
func newBoolStats() *boolStats {return &boolStats{}}
func (b *boolStats) NullCount() *int64 {return nil}
func (b *boolStats) DistinctCount() *int64 {return nil}
func (b *boolStats) Min() []byte {return nil}
func (b *boolStats) Max() []byte {return nil}
{{end}}`
