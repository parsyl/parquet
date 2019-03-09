package bitpack

func Pack(vals []int64) []byte {
	/*
		for vals = []int64{0,1,2,3,4,5,6,7}

		dec value: 0   1   2   3   4   5   6   7
		bit value: 000 001 010 011 100 101 110 111
		bit label: ABC DEF GHI JKL MNO PQR STU VWX

		output:
		bit value: 10001000 11000110 11111010
		bit label: HIDEFABC RMNOJKLG VWXSTUPQ
	*/
	return []byte{
		(byte(vals[0]&1) | //c
			byte((vals[0] & 2)) | //b
			byte((vals[0] & 4)) | //a
			byte((vals[1]&1)<<3) | //f
			byte((vals[1]&2)<<3) | //e
			byte((vals[1]&4)<<3) | //d
			byte((vals[2]&1)<<6) | //i
			byte((vals[2]&2)<<6)), //h
		(byte(vals[2]&4>>2) | //g
			byte((vals[3]&1)<<1) | //l
			byte((vals[3]&2)<<1) | //k
			byte((vals[3]&4)<<1) | //j
			byte((vals[4]&1)<<4) | //o
			byte((vals[4]&2)<<4) | //n
			byte((vals[4]&4)<<4) | //m
			byte((vals[5]&1)<<7)), //r
		(byte((vals[5]&2)>>1) | //q
			byte((vals[5]&4)>>1) | //p
			byte((vals[6]&1)<<2) | //u
			byte((vals[6]&2)<<2) | //t
			byte((vals[6]&4)<<2) | //s
			byte((vals[7]&1)<<5) | //x
			byte((vals[7]&2)<<5) | //w
			byte((vals[7]&4)<<5)), //v
	}
}
