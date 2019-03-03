package main

var stringOptionalTpl = `{{define "stringOptionalField"}}
type StringOptionalField struct {
	parquet.OptionalField
	vals []string
	val  func(r {{.Type}}) *string
	read func(r *{{.Type}}, v *string)
}

func NewStringOptionalField(val func(r {{.Type}}) *string, read func(r *{{.Type}}, v *string), col string) *StringOptionalField {
	return &StringOptionalField{
		val:           val,
		read:          read,
		OptionalField: parquet.NewOptionalField(col),
	}
}

func (f *StringOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Type: parquet.StringType, RepetitionType: parquet.RepetitionOptional}
}

func (f *StringOptionalField) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	if f.Defs[0] == 1 {
		var val *string
		v := f.vals[0]
		f.vals = f.vals[1:]
		val = &v
        f.read(r, val)
	}
	f.Defs = f.Defs[1:]
}

func (f *StringOptionalField) Add(r {{.Type}}) {
	v := f.val(r)
	if v != nil {
		f.vals = append(f.vals, *v)
		f.Defs = append(f.Defs, 1)
	} else {
		f.Defs = append(f.Defs, 0)
	}
}

func (f *StringOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals))
}

func (f *StringOptionalField) Read(r io.ReadSeeker, meta *parquet.Metadata, pg parquet.Page) error {
	start := len(f.Defs)
	rr, _, err := f.DoRead(r, meta, pg)
	if err != nil {
		return err
	}

	for j := 0; j < pg.N; j++ {
		if f.Defs[start+j] == 0 {
			continue
		}

		var x int32
		if err := binary.Read(rr, binary.LittleEndian, &x); err != nil {
			return err
		}
		s := make([]byte, x)
		if _, err := rr.Read(s); err != nil {
			return err
		}

		f.vals = append(f.vals, string(s))
	}
	return nil
}
{{end}}`
