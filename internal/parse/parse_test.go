package parse_test

type Being struct {
	ID  int32
	Age *int32
}

// type Person struct {
// 	Being
// 	Happiness   int64
// 	Sadness     *int64
// 	Code        string
// 	Funkiness   float32
// 	Lameness    *float32
// 	Keen        *bool
// 	Birthday    uint32
// 	Anniversary *uint64
// }

// type NewOrderPerson struct {
// 	Happiness int64
// 	Sadness   *int64
// 	Code      string
// 	Funkiness float32
// 	Lameness  *float32
// 	Keen      *bool
// 	Birthday  uint32
// 	Being
// 	Anniversary *uint64
// }

// type IgnoreMe struct {
// 	ID     int32  `parquet:"id"`
// 	Secret string `parquet:"-"`
// }

// type Tagged struct {
// 	ID   int32  `parquet:"id"`
// 	Name string `parquet:"name"`
// }

type Private struct {
	Being
	name string
}

type Nested2 struct {
	Info        Being
	Anniversary *uint64
}

type Nested struct {
	Being       Being
	Anniversary *uint64
}

type DoubleNested struct {
	Nested Nested
}

type OptionalNested struct {
	Being       *Being
	Anniversary *uint64
}

// type Thing struct {
// 	Name string
// }

// type OptionalNested2 struct {
// 	Being       *Thing
// 	Anniversary *uint64
// }

type OptionalDoubleNested struct {
	OptionalNested OptionalNested
}

// type Unsupported struct {
// 	Being
// 	// This field will be ignored because it's not one of the
// 	// supported types.
// 	Time time.Time
// }

// type SupportedAndUnsupported struct {
// 	Happiness int64
// 	x         int
// 	T1        time.Time
// 	Being
// 	y           int
// 	T2          time.Time
// 	Anniversary *uint64
// }

type Slice struct {
	IDs []int32 `parquet:"ids"`
}

type Slice2 struct {
	ID  int32   `parquet:"id"`
	IDs []int32 `parquet:"ids"`
}

// type Slice3 struct {
// 	ID  int32   `parquet:"id"`
// 	IDs []int32 `parquet:"ids"`
// 	Age *int32
// }

// type Hobby struct {
// 	Name string
// }

// type Slice4 struct {
// 	ID      int32 `parquet:"id"`
// 	Hobbies []Hobby
// }

// type Hobby2 struct {
// 	Names []string `parquet:"names"`
// }

// type Slice5 struct {
// 	ID    int32  `parquet:"id"`
// 	Hobby Hobby2 `parquet:"hobby"`
// }

// type Slice6 struct {
// 	ID      int32    `parquet:"id"`
// 	Hobbies []Hobby2 `parquet:"hobbies"`
// }

// type Slice7 struct {
// 	Thing *Slice6 `parquet:"thing"`
// }

// type Link struct {
// 	Backward []int64
// 	Forward  []int64
// }

// type Language struct {
// 	Code    string
// 	Country *string
// }

// type Name struct {
// 	Languages []Language
// 	URL       *string
// }

// type Document struct {
// 	DocID int64
// 	Links []Link
// 	Names []Name
// }

// type D struct {
// 	D int32
// }

// type C struct {
// 	D
// 	C string
// }

// type B struct {
// 	C
// 	B bool
// }

// type A struct {
// 	B
// 	Name string
// }
