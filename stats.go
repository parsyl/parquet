package parquet

type Stats interface {
	NullCount() *int64
	DistinctCount() *int64
	Min() []byte
	Max() []byte
	String() string
}
