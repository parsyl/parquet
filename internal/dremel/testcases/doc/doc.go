package doc

//go:generate parquetgen -input doc.go -type Document -package doc -output generated.go

type Link struct {
	Backward []int64 `parquet:"backward"`
	Forward  []int64 `parquet:"forward"`
}

type Language struct {
	Code    string  `parquet:"code"`
	Country *string `parquet:"country"`
}

type Name struct {
	Languages []Language `parquet:"languages"`
	URL       *string    `parquet:"url"`
}

type Document struct {
	DocID int64  `parquet:"docid"`
	Link  *Link  `parquet:"link"`
	Names []Name `parquet:"names"`
}
