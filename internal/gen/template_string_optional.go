package gen

var stringOptionalTpl = `{{define "stringOptionalField"}}
type StringOptionalField struct {
	parquet.OptionalField
	vals []string
	read   func(r {{.Type}}) ({{.TypeName}}, uint8, uint8)
	write  func(r *{{.Type}}, vals []{{removeStar .TypeName}}, def, rep uint8) bool
	stats *stringOptionalStats
}

func NewStringOptionalField(read func(r {{.Type}}) ({{.TypeName}}, uint8, uint8), write func(r *{{.Type}}, vals []{{removeStar .TypeName}}, def, rep uint8) bool, path []string, opts ...func(*parquet.OptionalField)) *StringOptionalField {
	return &StringOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, opts...),
		stats:         newStringOptionalStats(),
	}
}

func (f *StringOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: parquet.StringType, RepetitionType: f.RepetitionType}
}

func (f *StringOptionalField) Scan(r *{{.Type}}) {
	if len(f.Defs) == 0 {
		return
	}

	if f.write(r, f.vals, f.Defs[0]) {
		f.vals = f.vals[1:]
	}
	f.Defs = f.Defs[1:]
}

func (f *StringOptionalField) Add(r {{.Type}}) {
	v, def := f.read(r)
	f.stats.add(v)
	if v != nil {
		f.vals = append(f.vals, *v)

	}
	f.Defs = append(f.Defs, def)
}

func (f *StringOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
}

func (f *StringOptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	start := len(f.Defs)
	rr, _, err := f.DoRead(r, pg)
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

var stringOptionalStatsTpl = `{{define "stringOptionalStats"}}
type stringOptionalStats struct {
	vals []string
	min []byte
	max []byte
	nils int64
}

func newStringOptionalStats() *stringOptionalStats {
	return &stringOptionalStats{}
}

func (s *stringOptionalStats) add(val *string) {
	if val == nil {
		s.nils++
		return
	}
	s.vals = append(s.vals, *val)
}

func (s *stringOptionalStats) NullCount() *int64 {
	return &s.nils
}

func (s *stringOptionalStats) DistinctCount() *int64 {
	return nil
}

func (s *stringOptionalStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringOptionalStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringOptionalStats) minMax()  {
	if len(s.vals) == 0 {
		return
	}

	tmp := make([]string, len(s.vals))
	copy(tmp, s.vals)
	sort.Strings(tmp)
	s.min = []byte(tmp[0])
	s.max = []byte(tmp[len(tmp)-1])
}
{{end}}`
