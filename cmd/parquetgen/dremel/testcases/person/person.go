package person

//go:generate parquetgen -input person.go -type Person -package person -output generated.go

type Skill struct {
	Name       string `parquet:"name"`
	Difficulty string `parquet:"difficulty"`
}

type Hobby struct {
	Name       string  `parquet:"name"`
	Difficulty *int32  `parquet:"difficulty"`
	Skills     []Skill `parquet:"skills"`
}

type Person struct {
	Name  string `parquet:"name"`
	Hobby *Hobby `parquet:"hobby"`
}
