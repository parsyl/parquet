package bitpack

func Pack(width int, vals []int64) []byte {
	switch width {
	case 1:
		return packOne(vals)
	case 2:
		return packTwo(vals)
	case 3:
		return packThree(vals)
	default:
		return []byte{}
	}
}

func packOne(vals []int64) []byte {
	return []byte{
		(byte(vals[0]) |
			byte(vals[1]<<1) |
			byte(vals[2]<<2) |
			byte(vals[3]<<3) |
			byte(vals[4]<<4) |
			byte(vals[5]<<5) |
			byte(vals[6]<<6) |
			byte(vals[7]<<7)),
	}
}

/*
	for vals = []int64{0,1,2,3,0,1,2,3}

	dec value: 0  1  2  3  0  1  2  3
	bit value: 00 01 10 00 00 01 10 11
	bit label: AB CD EF GH IJ KL MN OP

	output:
	bit value:
	bit label: GHEFCDAB OPMNKLIJ
*/
func packTwo(vals []int64) []byte {
	return []byte{
		(byte(vals[0]&1) | //b
			byte((vals[0] & 2)) | //a
			byte((vals[1]&1)<<2) | //d
			byte((vals[1]&2)<<2) | //c
			byte((vals[2]&1)<<4) | //f
			byte((vals[2]&2)<<4) | //e
			byte((vals[3]&1)<<6) | //h
			byte((vals[3]&2)<<6)), //g
		(byte(vals[4]&1) | //j
			byte(vals[4]&2) | //i
			byte((vals[5]&1)<<2) | //l
			byte((vals[5]&2)<<2) | //k
			byte((vals[6]&1)<<4) | //n
			byte((vals[6]&2)<<4) | //m
			byte((vals[7]&1)<<6) | //p
			byte((vals[7]&2)<<6)), //o
	}
}

/*
	for vals = []int64{0,1,2,3,4,5,6,7}

	dec value: 0   1   2   3   4   5   6   7
	bit value: 000 001 010 011 100 101 110 111
	bit label: ABC DEF GHI JKL MNO PQR STU VWX

	output:
	bit value: 10001000 11000110 11111010
	bit label: HIDEFABC RMNOJKLG VWXSTUPQ
*/
func packThree(vals []int64) []byte {
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

func Unpack(width int, vals []byte) []int64 {
	switch width {
	case 1:
		return unpackOne(vals)
	case 2:
		return unpackTwo(vals)
	case 3:
		return unpackThree(vals)
	default:
		return []int64{}
	}
}

func unpackOne(vals []byte) []int64 {
	return []int64{
		int64(vals[0] & 1),
		int64((vals[0] & 2) >> 1),
		int64((vals[0] & 4) >> 2),
		int64((vals[0] & 8) >> 3),
		int64((vals[0] & 16) >> 4),
		int64((vals[0] & 32) >> 5),
		int64((vals[0] & 64) >> 6),
		int64((vals[0] & 128) >> 7),
	}
}

func unpackTwo(vals []byte) []int64 {
	return []int64{
		int64(vals[0] & 3),
		int64((vals[0] & (4 + 8) >> 2)),
		int64((vals[0] & (16 + 32) >> 4)),
		int64((vals[0] & (64 + 128)) >> 6),
		int64(vals[1] & 3),
		int64((vals[1] & (4 + 8) >> 2)),
		int64((vals[1] & (16 + 32) >> 4)),
		int64((vals[1] & (64 + 128)) >> 6),
	}
}

func unpackThree(vals []byte) []int64 {
	return []int64{
		int64(vals[0] & 7),
		int64((vals[0] & (8 + 16 + 32)) >> 3),
		int64((vals[0] & (64 + 128) >> 6) | (vals[1]&1)<<2),
		int64((vals[1] & (2 + 4 + 8)) >> 1),
		int64((vals[1] & (16 + 32 + 64)) >> 4),
		int64(((vals[1] & (128)) >> 7) | (vals[2]&3)<<1),
		int64((vals[2] & (4 + 8 + 16)) >> 2),
		int64((vals[2] & (32 + 64 + 128)) >> 5),
	}
}

func Unack(b byte) []int64 {

}
