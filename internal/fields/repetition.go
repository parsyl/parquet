package fields

// RepetitionType is an enum of the possible
// parquet repetition types
type RepetitionType int

const (
	Unseen   RepetitionType = -1
	Required RepetitionType = 0
	Optional RepetitionType = 1
	Repeated RepetitionType = 2
)

// RepetitionTypes provides several functions used by parquetgen's
// go templates to generate code.
type RepetitionTypes []RepetitionType

// Def returns the repetition type for the definition level
func (r RepetitionTypes) Def(def int) RepetitionType {
	var out RepetitionType
	var count int
	for _, rt := range r {
		if rt == Optional || rt == Repeated {
			count++
		}
		if count == def {
			out = rt
		}
	}

	return out
}

// MaxDef returns the largest definition level
func (r RepetitionTypes) MaxDef() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Optional || rt == Repeated {
			out++
		}
	}
	return out
}

// MaxRep returns the largest repetition level
func (r RepetitionTypes) MaxRep() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Repeated {
			out++
		}
	}
	return out
}

// Repeated figures out if there is a repeated field
func (r RepetitionTypes) Repeated() bool {
	for _, rt := range r {
		if rt == Repeated {
			return true
		}
	}
	return false
}

// Optional figures out if there is an optional field
func (r RepetitionTypes) Optional() bool {
	for _, rt := range r {
		if rt == Optional {
			return true
		}
	}
	return false
}

// Required figures out if there are no optional or repeated fields
func (r RepetitionTypes) Required() bool {
	for _, rt := range r {
		if rt != Required {
			return false
		}
	}
	return true
}

// NRepeated figures out if the sub-field at position i
// is repeated.
func (r RepetitionTypes) NRepeated(i int) bool {
	var count int
	for _, rt := range r {
		if rt == Repeated {
			count++
		}

		if count == i {
			return true
		}
	}
	return false
}

func reverse(in []field) []field {
	flds := append(in[:0:0], in...)
	for i := len(flds)/2 - 1; i >= 0; i-- {
		opp := len(flds) - 1 - i
		flds[i], flds[opp] = flds[opp], flds[i]
	}
	return flds
}

type rts []RepetitionType

func (r rts) add(i int, rts []RepetitionType) rts {
	if len(r) < i+1 {
		r = append(r, make([]RepetitionType, len(r)-i+1)...)
	}

	for _, rt := range rts {
		if rt > r[i] {
			r[i] = rt
		}
	}

	return r
}
