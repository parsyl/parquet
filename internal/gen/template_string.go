package gen

var stringTpl = `{{define "stringField"}}
type StringField struct {
	parquet.RequiredField
	vals []string
	read  func(r {{.Type}}) {{.TypeName}}
	write func(r *{{.Type}}, vals []{{removeStar .TypeName}})
	stats *stringStats
}

func NewStringField(read func(r {{.Type}}) {{.TypeName}}, write func(r *{{.Type}}, vals []{{removeStar .TypeName}}), path []string, opts ...func(*parquet.RequiredField)) *StringField {
	return &StringField{
		read:           read,
		write:          write,
		RequiredField: parquet.NewRequiredField(path, opts...),
		stats:         newStringStats(),
	}
}

func (f *StringField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: parquet.StringType, RepetitionType: parquet.RepetitionRequired}
}

func (f *StringField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.vals), f.stats)
}

func (f *StringField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	for j := 0; j < pg.N; j++ {
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

func (f *StringField) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *StringField) Add(r {{.Type}}) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}

func (f *StringField) Levels() ([]uint8, []uint8) {
	return nil, nil
}
{{end}}`

var stringStatsTpl = `{{define "stringStats"}}
type stringStats struct {
	vals []string
	min []byte
	max []byte
}

func newStringStats() *stringStats {
	return &stringStats{}
}

func (s *stringStats) add(val string) {
	s.vals = append(s.vals, val)
}

func (s *stringStats) NullCount() *int64 {
	return nil
}

func (s *stringStats) DistinctCount() *int64 {
	return nil
}

func (s *stringStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringStats) minMax()  {
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
