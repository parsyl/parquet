package parse_test

type Being struct {
	ID  int32
	Age *int32
}

type Person struct {
	Being
	Happiness   int64
	Sadness     *int64
	Code        string
	Funkiness   float32
	Lameness    *float32
	Keen        *bool
	Birthday    uint32
	Anniversary *uint64
}

type NewOrderPerson struct {
	Happiness int64
	Sadness   *int64
	Code      string
	Funkiness float32
	Lameness  *float32
	Keen      *bool
	Birthday  uint32
	Being
	Anniversary *uint64
}

type IgnoreMe struct {
	ID     int32  `parquet:"id"`
	Secret string `parquet:"-"`
}

type Tagged struct {
	ID   int32  `parquet:"id"`
	Name string `parquet:"name"`
}

type Private struct {
	Being
	name string
}
