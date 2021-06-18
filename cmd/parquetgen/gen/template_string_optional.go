package gen

var stringOptionalTpl = `{{define "stringOptionalField"}}
type StringOptionalField struct {
	parquet.OptionalField
	vals []string
	read   func(r {{.StructType}}) ([]{{removeStar .TypeName}}, []uint8, []uint8)
	write  func(r *{{.StructType}}, vals []{{removeStar .TypeName}}, def, rep []uint8) (int, int)
	stats *stringOptionalStats
}

func NewStringOptionalField(read func(r {{.StructType}}) ([]{{removeStar .TypeName}}, []uint8, []uint8), write func(r *{{.StructType}}, vals []{{removeStar .TypeName}}, defs, reps []uint8) (int, int), path []string, types []int, opts ...func(*parquet.OptionalField)) *StringOptionalField {
	return &StringOptionalField{
		read:          read,
		write:         write,
		OptionalField: parquet.NewOptionalField(path, types, opts...),
		stats:         newStringOptionalStats(maxDef(types)),
	}
}

func (f *StringOptionalField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: StringType, RepetitionType: f.RepetitionType, Types: f.Types}
}

func (f *StringOptionalField) Add(r {{.StructType}}) {
	vals, defs, reps := f.read(r)
	f.stats.add(vals, defs)
	f.vals = append(f.vals, vals...)
	f.Defs = append(f.Defs, defs...)
	f.Reps = append(f.Reps, reps...)
}

func (f *StringOptionalField) Scan(r *{{.StructType}}) {
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

func (f *StringOptionalField) Write(w io.Writer, meta *parquet.Metadata) error {
	buf := bytes.Buffer{}

	for _, s := range f.vals {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(s))); err != nil {
			return err
		}
		buf.Write([]byte(s))
	}

	return f.DoWrite(w, meta, buf.Bytes(), len(f.Defs), f.stats)
}

func (f *StringOptionalField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, _, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	for j := 0; j < f.Values(); j++ {
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

func (f *StringOptionalField) Levels() ([]uint8, []uint8) {
	return f.Defs, f.Reps
}
{{end}}`

var stringOptionalStatsTpl = `{{define "stringOptionalStats"}}
type stringOptionalStats struct {
	vals []string
	min []byte
	max []byte
	nils int64
	maxDef uint8
}

func newStringOptionalStats(d uint8) *stringOptionalStats {
	return &stringOptionalStats{maxDef: d}
}

func (s *stringOptionalStats) add(vals []string, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < s.maxDef {
			s.nils++
		} else {
			s.vals = append(s.vals, vals[i])
			i++
		}
	}
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
