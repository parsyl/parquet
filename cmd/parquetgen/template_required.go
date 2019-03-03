package main

var requiredTpl = `{{define "requiredField"}}
type {{.FieldType}} struct {
	vals []{{.TypeName}}
	parquet.RequiredField
	val  func(r {{.Type}}) {{.TypeName}}
	read func(r *{{.Type}}, v {{.TypeName}})
}

func New{{.FieldType}}(val func(r {{.Type}}) {{.TypeName}}, read func(r *{{.Type}}, v {{.TypeName}}), col string) *{{.FieldType}} {
	return &{{.FieldType}}{
		val:           val,
		read:          read,
		RequiredField: parquet.NewRequiredField(col),
	}
}

func (f *{{.FieldType}}) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.{{.ParquetType}}, RepetitionType: parquet.RepetitionRequired}
}

func (f *{{.FieldType}}) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}
	v := f.vals[0]
	f.vals = f.vals[1:]
	f.read(r, v)
}

func (f *{{.FieldType}}) Write(w io.Writer, meta *parquet.Metadata) error {
	var buf bytes.Buffer
	for _, v := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *{{.FieldType}}) Read(r io.ReadSeeker, meta *parquet.Metadata, pos parquet.Position) error {
	rr, _, err := f.DoRead(r, meta, pos)
	if err != nil {
		return err
	}

	v := make([]{{.TypeName}}, int(pos.N))
	err = binary.Read(rr, binary.LittleEndian, &v)
	f.vals = append(f.vals, v...)
	return err
}

func (f *{{.FieldType}}) Add(r {{.Type}}) {
	f.vals = append(f.vals, f.val(r))
}
{{end}}`
