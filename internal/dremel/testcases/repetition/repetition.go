package repetition

//go:generate parquetgen -input repetition.go -type Document -package repetition -output generated.go

type (
	Document struct {
		Links []Link `parquet:"links"`
	}

	Link struct {
		Backward []Language `parquet:"backward"`
		Forward  []Language `parquet:"forward"`
	}

	Language struct {
		Codes     []string `parquet:"code"`
		Countries []string `parquet:"countries"`
	}
)
