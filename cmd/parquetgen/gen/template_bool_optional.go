package gen

var boolOptionalTpl = `{{define "boolOptionalField"}}type BoolOptionalField struct {
	parquet.OptionalField
	vals  []bool
	read   func(r {{.StructType}}, vals []{{removeStar .TypeName}}, defs, reps []uint8) ([]{{removeStar .TypeName}}, []uint8, []uint8)
	write  func(r *{{.StructType}}, vals []{{removeStar .TypeName}}, defs, reps []uint8) (int, int)
	stats *boolOptionalStats
}

func NewBoolOptionalField(read func(r {{.StructType}}, vals []{{removeStar .TypeName}}, defs, reps []uint8) ([]{{removeStar .TypeName}}, []uint8, []uint8), write func(r *{{.StructType}}, vals []{{removeStar .TypeName}}, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *BoolOptionalField {
	return &BoolOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newBoolOptionalStats(maxDef(types)),
	}
}

func (f *BoolOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: BoolType, RepetitionType: f.RepetitionType, Types: f.Types}
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

func (f *BoolOptionalField) Scan(r *{{.StructType}}) {
	if len(f.Defs) == 0 {
		return
	}

	v, l := f.write(r, f.vals, f.Defs, f.Reps)
	f.vals = f.vals[v:]
	f.Defs = f.Defs[l:]
	if len(f.Reps) > 0 {
		f.Reps = f.Reps[l:]
	}
}

func (f *BoolOptionalField) Add(r {{.StructType}}) {
	vals, defs, reps := f.read(r, f.vals, f.Defs, f.Reps)
	f.stats.add(vals[len(f.vals):], defs[len(f.Defs):])
	f.vals = vals
	f.Defs = defs
	f.Reps = reps
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

	return f.DoWrite(w, meta, rawBuf, len(f.Defs), f.stats)
}

func (f *BoolOptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}
{{end}}`

var boolOptionalStatsTpl = `{{define "boolOptionalStats"}}
type boolOptionalStats struct {
	maxDef uint8
	nils int64
}

func newBoolOptionalStats(d uint8) *boolOptionalStats {
	return &boolOptionalStats{maxDef: d}
}

func (b *boolOptionalStats) add(vals []bool, defs []uint8) {
	for _, def := range defs {
		if def < b.maxDef {
			b.nils++
		}
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
