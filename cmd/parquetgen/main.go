package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"

	"github.com/parsyl/parquet"
	"github.com/parsyl/parquet/cmd/parquetgen/gen"
	sch "github.com/parsyl/parquet/schema"
)

var (
	metadata     = flag.Bool("metadata", false, "print the metadata of a parquet file (-parquet) and exit")
	pageheaders  = flag.Bool("pageheaders", false, "print the page headers of a parquet file (-parquet) and exit (also prints the metadata)")
	typ          = flag.String("type", "", "name of the struct that will used for writing and reading")
	pkg          = flag.String("package", "", "package of the generated code")
	imp          = flag.String("import", "", "import statement of -type if it doesn't live in -package")
	pth          = flag.String("input", "", "path to the go file that defines -type")
	outPth       = flag.String("output", "parquet.go", "name of the file that is produced, defaults to parquet.go")
	ignore       = flag.Bool("ignore", true, "ignore unsupported fields in -type, otherwise log.Fatal is called when an unsupported type is encountered")
	parq         = flag.String("parquet", "", "path to a parquet file (if you are generating code based on an existing parquet file or printing the file metadata or page headers)")
	structOutPth = flag.String("struct-output", "generated_struct.go", "name of the file that is produced, defaults to parquet.go")
)

func main() {
	flag.Parse()

	if *pth != "" && *parq != "" {
		log.Fatal("choose -parquet or -input, but not both")
	}

	var err error
	if *metadata {
		readFooter()
	} else if *pageheaders {
		readPageHeaders()
	} else if *parq == "" {
		err = gen.FromStruct(*pth, *outPth, *typ, *pkg, *imp, *ignore)
	} else {
		err = gen.FromParquet(*parq, *structOutPth, *outPth, *typ, *pkg, *imp, *ignore)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func readPageHeaders() {
	f := openParquet()
	footer := getFooter(f)

	pageHeaders, err := parquet.PageHeaders(footer, f)
	if err != nil {
		log.Fatal(err)
	}

	f.Close()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(struct {
		PageHeaders []sch.PageHeader `json:"page_headers"`
		MetaData    sch.FileMetaData `json:"file_metadata"`
	}{
		PageHeaders: pageHeaders,
		MetaData:    *footer,
	})
}

func readFooter() {
	f := openParquet()
	footer := getFooter(f)
	f.Close()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(footer)
}

func openParquet() *os.File {
	if *parq == "" {
		log.Fatal("-parquet is required with -footer")
	}

	f, err := os.Open(*parq)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func getFooter(r io.ReadSeeker) *sch.FileMetaData {
	footer, err := parquet.ReadMetaData(r)
	if err != nil {
		log.Fatal("couldn't read footer: ", err)
	}
	return footer
}
