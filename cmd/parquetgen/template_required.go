package main

var requiredNumericTpl = `{{define "numericField"}}
type {{.FieldType}} struct {
	vals []{{.TypeName}}
	parquet.RequiredField
	read  func(r {{.Type}}) ({{.TypeName}})
	write func(r *{{.Type}}, vals []{{removeStar .TypeName}})
	stats *{{.TypeName}}stats
}

{{writeFunc .}}

func New{{.FieldType}}(read func(r {{.Type}}) ({{.TypeName}}), write func(r *{{.Type}}, vals []{{removeStar .TypeName}}), col string, opts ...func(*parquet.RequiredField)) *{{.FieldType}} {
	return &{{.FieldType}}{
		read:           read,
		write:          write,
		RequiredField: parquet.NewRequiredField(col, opts...),
		stats:         new{{camelCase .TypeName}}stats(),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.{{.ParquetType}}, RepetitionType: parquet.RepetitionRequired}
}

func (f *{{.FieldType}}) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	v := make([]{{.TypeName}}, int(pg.N))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
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

func (f *{{.FieldType}}) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *{{.FieldType}}) Add(r {{.Type}}) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}
{{end}}`

var requiredStatsTpl = `{{define "requiredStats"}}
type {{.TypeName}}stats struct {
	min {{.TypeName}}
	max {{.TypeName}}
}

func new{{camelCase .TypeName}}stats() *{{.TypeName}}stats {
	return &{{.TypeName}}stats{
		min: {{.TypeName}}(math.Max{{camelCase .TypeName}}),
	}
}

func (i *{{.TypeName}}stats) add(val {{.TypeName}}) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *{{.TypeName}}stats) bytes(val {{.TypeName}}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *{{.TypeName}}stats) NullCount() *int64 {
	return nil
}

func (f *{{.TypeName}}stats) DistinctCount() *int64 {
	return nil
}

func (f *{{.TypeName}}stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *{{.TypeName}}stats) Max() []byte {
	return f.bytes(f.max)
}
{{end}}`
