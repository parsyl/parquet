package fields

type RepetitionType int

const (
	Unseen   RepetitionType = -1
	Required RepetitionType = 0
	Optional RepetitionType = 1
	Repeated RepetitionType = 2
)

type RepetitionTypes []RepetitionType

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

func (r RepetitionTypes) MaxDef() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Optional || rt == Repeated {
			out++
		}
	}
	return out
}

func (r RepetitionTypes) MaxRep() uint8 {
	var out uint8
	for _, rt := range r {
		if rt == Repeated {
			out++
		}
	}
	return out
}

func (r RepetitionTypes) Repeated() bool {
	for _, rt := range r {
		if rt == Repeated {
			return true
		}
	}
	return false
}

func (r RepetitionTypes) Optional() bool {
	for _, rt := range r {
		if rt == Optional {
			return true
		}
	}
	return false
}

func (r RepetitionTypes) Required() bool {
	for _, rt := range r {
		if rt != Required {
			return false
		}
	}
	return true
}

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
