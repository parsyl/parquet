package main

var optionalTpl = `{{define "optionalField"}}
type {{.FieldType}} struct {
	parquet.OptionalField
	vals []{{removeStar .TypeName}}
	read func(r *{{.Type}}, v {{.TypeName}})
	val  func(r {{.Type}}) {{.TypeName}}
}

func New{{.FieldType}}(val func(r {{.Type}}) {{.TypeName}}, read func(r *{{.Type}}, v {{.TypeName}}), col string, opts ...func(*parquet.OptionalField)) *{{.FieldType}} {
	return &{{.FieldType}}{
		val:           val,
		read:          read,
		OptionalField: parquet.NewOptionalField(col, opts...),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.{{.ParquetType}}, RepetitionType: parquet.RepetitionOptional}
}

func (f *{{.FieldType}}) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer

	min := {{removeStar .TypeName}}({{maxType .}})
	var max {{removeStar .TypeName}}
	for _, v := range f.vals {
		min, max = f.minMax(v, min, max)
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *{{.FieldType}}) minMax(val, min, max {{removeStar .TypeName}}) ({{removeStar .TypeName}}, {{removeStar .TypeName}}) {
	if val < min {
		min = val
	}
	if val > max {
		max = val
	}
	return min, max
}

func (f *{{.FieldType}}) stats(min, max {{removeStar .TypeName}}) *sch.Statistics {
	return &sch.Statistics{
		MinValue:  f.minMaxBytes(min),
		MaxValue:  f.minMaxBytes(max),
		NullCount: f.NilCount(),
	}
}

func (f *{{.FieldType}}) minMaxBytes(val {{removeStar .TypeName}}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *{{.FieldType}}) Read(r io.ReadSeeker, meta *parquet.Metadata, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, meta, pg)
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
