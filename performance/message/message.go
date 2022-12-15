package message

// base
// go run cmd/parquetgen/main.go -input performance/message/message.go -type Message -package base  -output performance/base/parquet.go -import github.com/inigolabs/parquet/performance/message
// optimized
// go run cmd/parquetgen/main.go -input performance/message/message.go -type Message -package performance  -output performance/parquet.go -import github.com/inigolabs/parquet/performance/message
type Message struct {
	ColStr0 *string `parquet:"col_str_0" json:"col_str_0" faker:"word"`
	ColStr1 string  `parquet:"col_str_1" json:"col_str_1" faker:"oneof: aaaaa, "` // optionally empty
	ColStr2 *string `parquet:"col_str_2" json:"col_str_2" faker:"paragraph"`
	ColStr3 string  `parquet:"col_str_3" json:"col_str_3" faker:"paragraph"`
	ColStr4 *string `parquet:"col_str_4" json:"col_str_4" faker:"sentence"`
	ColStr5 string  `parquet:"col_str_5" json:"col_str_5" faker:"sentence"`
	ColStr6 *string `parquet:"col_str_6" json:"col_str_6" faker:"sentence"`
	ColStr7 string  `parquet:"col_str_7" json:"col_str_7" faker:"word"`
	ColStr8 *string `parquet:"col_str_8" json:"col_str_8" faker:"word"`
	ColStr9 string  `parquet:"col_str_9" json:"col_str_9" faker:"word"`

	ColInt0 *int64 `parquet:"col_int_0" json:"col_int_0" faker:"unix_time"`
	ColInt1 int64  `parquet:"col_int_1" json:"col_int_1" faker:"oneof: 0, 1"`
	ColInt2 *int64 `parquet:"col_int_2" json:"col_int_2" faker:"unix_time"`
	ColInt3 int64  `parquet:"col_int_3" json:"col_int_3" faker:"unix_time"`
	ColInt4 *int64 `parquet:"col_int_4" json:"col_int_4" faker:"unix_time"`

	ColInt32_0 *int32 `parquet:"col_int_32_0" json:"col_int_32_0"`
	ColInt32_1 int32  `parquet:"col_int_32_1" json:"col_int_32_1" faker:"oneof: 0, 1"`
	ColInt32_2 *int32 `parquet:"col_int_32_2" json:"col_int_32_2"`
	ColInt32_3 int32  `parquet:"col_int_32_3" json:"col_int_32_3"`
	ColInt32_4 *int32 `parquet:"col_int_32_4" json:"col_int_32_4"`

	ColFloat0 *float64 `parquet:"col_float_0" json:"col_float_0"`
	ColFloat1 float64  `parquet:"col_float_1" json:"col_float_1"`
	ColFloat2 *float64 `parquet:"col_float_2" json:"col_float_2"`
	ColFloat3 float64  `parquet:"col_float_3" json:"col_float_3"`
	ColFloat4 *float64 `parquet:"col_float_4" json:"col_float_4"`

	ColFloat32_0 *float32 `parquet:"col_float_32_0" json:"col_float_32_0"`
	ColFloat32_1 float32  `parquet:"col_float_32_1" json:"col_float_32_1" faker:"oneof: 0.0, 1.1"`
	ColFloat32_2 *float32 `parquet:"col_float_32_2" json:"col_float_32_2"`
	ColFloat32_3 float32  `parquet:"col_float_32_3" json:"col_float_32_3"`
	ColFloat32_4 *float32 `parquet:"col_float_32_4" json:"col_float_32_4"`

	ColBool0 *bool `parquet:"col_bool_0" json:"col_bool_0"`
	ColBool1 bool  `parquet:"col_bool_1" json:"col_bool_1"`
	ColBool2 *bool `parquet:"col_bool_2" json:"col_bool_2"`
	ColBool3 bool  `parquet:"col_bool_3" json:"col_bool_3"`
	ColBool4 *bool `parquet:"col_bool_4" json:"col_bool_4"`
	ColBool5 bool  `parquet:"col_bool_5" json:"col_bool_5"`
	ColBool6 *bool `parquet:"col_bool_6" json:"col_bool_6"`
	ColBool7 bool  `parquet:"col_bool_7" json:"col_bool_7"`
	ColBool8 *bool `parquet:"col_bool_8" json:"col_bool_8"`
	ColBool9 bool  `parquet:"col_bool_9" json:"col_bool_9"`
}
