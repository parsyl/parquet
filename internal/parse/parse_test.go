package parse_test

type Being struct {
	ID  int32 `parquet:"id"`
	Age *int32
}

type Person struct {
	Being
	Happiness   int64
	Sadness     *int64 `parquet:"sadness"`
	Code        string
	Funkiness   float32
	Lameness    *float32
	Keen        *bool
	Birthday    uint32
	Anniversary *uint64
}

type IgnoreMe struct {
	ID     int32  `parquet:"id"`
	Secret string `parquet:"-"`
}
