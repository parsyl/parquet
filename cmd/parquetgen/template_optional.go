package main

var writeSwitchTpl = `{{define "writeSwitch"}}
{{end}}`

var optionalTpl = `{{define "optionalField"}}
type {{.FieldType}} struct {
	parquet.OptionalField
	vals  []{{removeStar .TypeName}}
	read  func(r *{{.Type}}, v {{.TypeName}})
	val   func(r {{.Type}}) {{.TypeName}}
	stats {{.TypeName}}optionalStats
}

func write{{funcName .}}(r {{.Type}}, v {{.TypeName}}, def int64) {
	switch def {
        {{template "writeSwitch" .}}
	}
}

func New{{.FieldType}}(val func(r {{.Type}}) {{.TypeName}}, read func(r *{{.Type}}, v {{.TypeName}}), col string, opts ...func(*parquet.OptionalField)) *{{.FieldType}} {
	return &{{.FieldType}}{
		val:           val,
		read:          read,
		OptionalField: parquet.NewOptionalField(col, opts...),
		stats:         new{{removeStar .TypeName}}optionalStats(),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.{{.ParquetType}}, RepetitionType: parquet.RepetitionOptional}
}

func (f *{{.FieldType}}) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
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
	v := f.val(r)
	f.stats.add(v)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.Defs = append(f.Defs, 1)
	} else {
		f.Defs = append(f.Defs, 0)
	}
}

func (f *{{.FieldType}}) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	if f.Defs[0] == 1 {
        var val {{removeStar .TypeName}}
		v := f.vals[0]
		f.vals = f.vals[1:]
		val = v
        f.read(r, &val)
	}
	f.Defs = f.Defs[1:]
}
{{end}}`

var optionalStatsTpl = `{{define "optionalStats"}}
type {{removeStar .TypeName}}optionalStats struct {
	min {{removeStar .TypeName}}
	max {{removeStar .TypeName}}
	nils int64
	nonNils int64
}

func new{{removeStar .TypeName}}optionalStats() {{.TypeName}}optionalStats {
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
