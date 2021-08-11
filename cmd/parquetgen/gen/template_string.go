package gen

var stringTpl = `{{define "stringField"}}
type StringField struct {
	parquet.RequiredField
	vals []string
	read  func(r {{.StructType}}) {{.TypeName}}
	write func(r *{{.StructType}}, vals []{{removeStar .TypeName}})
	stats *stringStats
}

func NewStringField(read func(r {{.StructType}}) {{.TypeName}}, write func(r *{{.StructType}}, vals []{{removeStar .TypeName}}), path []string, opts ...func(*parquet.RequiredField)) *StringField {
	return &StringField{
		read:           read,
		write:          write,
		RequiredField: parquet.NewRequiredField(path, opts...),
		stats:         newStringStats(),
	}
}

func (f *StringField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: StringType, RepetitionType: parquet.RepetitionRequired, Types: []int{0}}
}

func (f *StringField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := buffpool.Get()
	defer buffpool.Put(buf)

	bs := make([]byte, 4)
	for _, s := range f.vals {
		binary.LittleEndian.PutUint32(bs, uint32(len(s)))
		if _, err := buf.Write(bs); err != nil {
			return err
		}
		buf.WriteString(s)
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

func (f *StringField) Scan(r *{{.StructType}}) {
	if len(f.vals) == 0 {
		return
	}

	f.write(r, f.vals)
	f.vals = f.vals[1:]
}

func (f *StringField) Add(r {{.StructType}}) {
	v := f.read(r)
	f.stats.add(v)
	f.vals = append(f.vals, v)
}

func (f *StringField) Levels() ([]uint8, []uint8) {
	return nil, nil
}
{{end}}`

var stringStatsTpl = `{{define "stringStats"}}

const nilString = "__#NIL#__"

type stringStats struct {
	min string
	max string
}

func newStringStats() *stringStats {
	return &stringStats{
		min: nilString,
		max: nilString,
	}
}

func (s *stringStats) add(val string) {
	if s.min == nilString {
		s.min = val
	} else {
		if val < s.min {
			s.min = val
		}
	}
	if s.max == nilString {
		s.max = val
	} else {
		if val > s.max {
			s.max = val
		}
	}
}

func (s *stringStats) NullCount() *int64 {
	return nil
}

func (s *stringStats) DistinctCount() *int64 {
	return nil
}

func (s *stringStats) Min() []byte {
	if s.min == nilString {
		return nil
	}
	return []byte(s.min)
}

func (s *stringStats) Max() []byte {
	if s.max == nilString {
		return nil
	}
	return []byte(s.max)
}
{{end}}`
