package gen

var readTpl = `{{define "readFunc"}}
{{end}}`

var optionalNumericTpl = `{{define "optionalField"}}
type {{.FieldType}} struct {
	parquet.OptionalField
	vals  []{{removeStar .TypeName}}
	read   func(r {{.Type}}) ([]{{removeStar .TypeName}}, []uint8, []uint8)
	write  func(r *{{.Type}}, vals []{{removeStar .TypeName}}, def, rep []uint8) (int, int)
	stats {{.TypeName}}optionalStats
}

func New{{.FieldType}}(read func(r {{.Type}}) ([]{{removeStar .TypeName}}, []uint8, []uint8), write func(r *{{.Type}}, vals []{{removeStar .TypeName}}, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *{{.FieldType}} {
	return &{{.FieldType}}{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		//stats:         new{{removeStar .TypeName}}optionalStats(),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: parquet.{{.ParquetType}}, RepetitionType: f.RepetitionType}
}

func (f *{{.FieldType}}) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), nil)
}

func (f *{{.FieldType}}) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v := make([]{{removeStar .TypeName}}, f.Values()-len(f.vals))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *{{.FieldType}}) Add(r {{.Type}}) {
	vals, defs, reps := f.read(r)
	//f.stats.add(v)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *{{.FieldType}}) Scan(r *{{.Type}}) {
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

func (f *{{.FieldType}}) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}
{{end}}`

var optionalStatsTpl = `{{define "optionalStats"}}
type {{removeStar .TypeName}}optionalStats struct {
	min {{removeStar .TypeName}}
	max {{removeStar .TypeName}}
	nils int64
	nonNils int64
}

func new{{removeStar .TypeName}}optionalStats() *{{removeStar .TypeName}}optionalStats {
	return &{{removeStar .TypeName}}optionalStats{
		min: {{removeStar .TypeName}}(math.Max{{camelCaseRemoveStar .TypeName}}),
	}
}

func (f *{{removeStar .TypeName}}optionalStats) add(val *{{removeStar .TypeName}}) {
	if val == nil {
		f.nils++
		return
	}

	f.nonNils++
	if *val < f.min {
		f.min = *val
	}
	if *val > f.max {
		f.max = *val
	}
}

func (f *{{removeStar .TypeName}}optionalStats) bytes(val {{removeStar .TypeName}}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *{{removeStar .TypeName}}optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *{{removeStar .TypeName}}optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *{{removeStar .TypeName}}optionalStats) Min() []byte {
	if f.nonNils == 0  {
		return nil
	}
	return f.bytes(f.min)
}

func (f *{{removeStar .TypeName}}optionalStats) Max() []byte {
	if f.nonNils == 0  {
		return nil
	}
	return f.bytes(f.max)
}
{{end}}`
