package gen

var boolTpl = `{{define "boolField"}}type BoolField struct {
	{{parquetType .}}
	vals []bool
	read  func(r {{.Type}}) {{.TypeName}}
	write func(r *{{.Type}}, vals []{{removeStar .TypeName}})
    stats *boolStats
}

func NewBoolField(read func(r {{.Type}}) {{.TypeName}}, write func(r *{{.Type}}, vals []{{removeStar .TypeName}}), path []string, opts ...func(*{{parquetType .}})) *BoolField {
	return &BoolField{
		read:          read,
		write:         write,
		RequiredField: parquet.NewRequiredField(path, opts...),
	}
}

func (f *BoolField) Schema() parquet.Field {
	return parquet.Field{Name: f.Name(), Path: f.Path(), Type: parquet.BoolType, RepetitionType: parquet.RepetitionRequired}
}


func (f *BoolField) Write(w io.Writer, meta *parquet.Metadata) error {
	ln := len(f.vals)
	n := (ln + 7) / 8
	rawBuf := make([]byte, n)

	for i := 0; i < ln; i++ {
		if f.vals[i] {
			rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
		}
	}

	return f.DoWrite(w, meta, rawBuf, len(f.vals), newBoolStats())
}

func (f *BoolField) Read(r io.ReadSeeker, pg parquet.Page) error {
	rr, sizes, err := f.DoRead(r, pg)
	if err != nil {
		return err
	}

	f.vals, err = parquet.GetBools(rr, int(pg.N), sizes)
	return err
}

func (f *BoolField) Scan(r *{{.Type}}) {
	if len(f.vals) == 0 {
		return
	}
	
	f.write(r, f.vals)
    f.vals = f.vals[1:]
}

func (f *BoolField) Add(r {{.Type}}) {
	v := f.read(r)
	f.vals = append(f.vals, v)
}

func (f *BoolField) Levels() ([]uint8, []uint8) {
	return nil, nil
}
{{end}}`

var boolStatsTpl = `{{define "boolStats"}}
type boolStats struct {}
func newBoolStats() *boolStats {return &boolStats{}}
func (b *boolStats) NullCount() *int64 {return nil}
func (b *boolStats) DistinctCount() *int64 {return nil}
func (b *boolStats) Min() []byte {return nil}
func (b *boolStats) Max() []byte {return nil}
{{end}}`
