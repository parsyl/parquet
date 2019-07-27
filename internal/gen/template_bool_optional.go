package gen

var boolOptionalTpl = `{{define "boolOptionalField"}}type BoolOptionalField struct {
	parquet.OptionalField
	vals  []bool
	read   func(r {{.Type}}) ({{.TypeName}}, uint8, uint8)
	write  func(r *{{.Type}}, vals []{{removeStar .TypeName}}, def, rep uint8) bool
	stats *boolOptionalStats
}

func NewBoolOptionalField(read func(r {{.Type}}) ({{.TypeName}}, uint8, uint8), write func(r *{{.Type}}, vals []{{removeStar .TypeName}}, def, rep uint8) bool, path []string, opts ...func(*parquet.OptionalField)) *BoolOptionalField {
	return &BoolOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, opts...),
		stats:         newBoolOptionalStats(),
	}
}

func (f *BoolOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: parquet.BoolType, RepetitionType: f.RepetitionType}
}

func (f *BoolOptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, sizes, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v, err := parquet.GetBools(rr, f.Values()-len(f.vals), sizes)
	f.vals = append(f.vals, v...)
	return err
}

func (f *BoolOptionalField) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	if f.write(r, f.vals, f.Defs[0]) {
		f.vals = f.vals[1:]
	}
	f.Defs = f.Defs[1:]
}

func (f *BoolOptionalField) Add(r {{.Type}}) {
	v, def := f.read(r)
	f.stats.add(v)
	if v != nil {
		f.vals = append(f.vals, *v)

	}
	f.Defs = append(f.Defs, def)
}

func (f *BoolOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	byteNum := (ln + 7) / 8
	rawBuf := make([]byte, byteNum)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals), f.stats)
}
{{end}}`

var boolOptionalStatsTpl = `{{define "boolOptionalStats"}}
type boolOptionalStats struct {
	nils int64
}

func newBoolOptionalStats() *boolOptionalStats {
	return &boolOptionalStats{}
}

func (b *boolOptionalStats) add(val *bool) {
	if val == nil {
		b.nils++	
	}
}

func (b *boolOptionalStats) NullCount() *int64 {
	return &b.nils
}

func (b *boolOptionalStats) DistinctCount() *int64 {
	return nil
}

func (b *boolOptionalStats) Min() []byte {
	return nil
}

func (b *boolOptionalStats) Max() []byte {
	return nil
}
{{end}}`
