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

const nilOptString = "__#NIL#__"

type stringOptionalStats struct {
	min    string
	max    string
	nils int64
	maxDef uint8
}

func newStringOptionalStats(d uint8) *stringOptionalStats {
	return &stringOptionalStats{
		min:    nilOptString,
		max:    nilOptString,
		maxDef: d,
	}
}

func (s *stringOptionalStats) add(vals []string, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < s.maxDef {
			s.nils++
		} else {
			val := vals[i]
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
	if s.min == nilOptString {
		return nil
	}
	return []byte(s.min)
}

func (s *stringOptionalStats) Max() []byte {
	if s.max == nilOptString {
		return nil
	}
	return []byte(s.max)
}
{{end}}`
