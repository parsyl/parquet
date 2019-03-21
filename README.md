# Parquet

Parquet generates a parquet reader and writer for a single struct.  The struct
can be defined by you or it can be generated by reading an existing parquet file.

NOTE: If you generate the code based on a parquet file there are quite a few
limitations.  The PageType of each PageHeader must be DATA_PAGE and the Codec
(defined in ColumnMetaData) must be PLAIN or SNAPPY. Also, the parquet file's
schema must consist of the currently [supported types](#supported-types).  But
wait, there's more!  Some of the encodings, like DELTA_BINARY_PACKED, BIT_PACKED,
PLAIN_DICTIONARY, and DELTA_BYTE_ARRAY are also not supported.  I would guess
there are other parquet options that will cause problems since there are so many
possibilities.

## Installation

    go get -u github.com/parsyl/parquet/...

## Usage

First define a struct for the data to be written to parquet:

```go
type Person struct {
  	ID  int32  `parquet:"id"`
	Age *int32 `parquet:"age"`
}
```

Next, add a go:generate comment somewhere (in this example all code lives
in main.go):

```go
//go:generate parquetgen -input main.go -type Person -package main
```

Generate the code for the reader and writer:

```console
$ go generate
```

A new file (parquet.go) has now been written that defines ParquetWriter
and ParquetReader.  Next, make use of the writer and reader:

```go
package main

import (
    "bytes"
    "encoding/json"
)

func main() {
    var buf bytes.Buffer
    w, err := NewParquetWriter(&buf)
    if err != nil {
        log.Fatal(err)
    }

    w.Add(Person{ID: 1, Age: getAge(30)})
    w.Add(Person{ID: 2})

    // Each call to write creates a new parquet row group.
    if err := w.Write(); err != nil {
        log.Fatal(err)
    }

    // Close must be called when you are done.  It writes
    // the parquet metadata at the end of the file.
    if err := w.Close(); err != nil {
        log.Fatal(err)
    }

    r, err := NewParquetReader(bytes.NewReader(buf.Bytes()))
    if err != nil {
        log.Fatal(err)
    }

    enc := json.NewEncoder(os.Stdout)
    for r.Next() {
        var p Person
        r.Scan(&p)
        enc.Encode(p)
    }

    if err := r.Error(); err != nil {
        log.Fatal(err)
    }
}

func getAge(a int32) *int32 { return &a }
```

NewParquetWriter has a couple of optional arguments available: MaxPageSize,
Uncompressed, and Snappy.  For example, the following sets the page size (number
of rows in a page before a new one is created) and sets the page data compression
to snappy:

```go
    w, err := NewParquetWriter(&buf, MaxPageSize(10000), Snappy)
```

See [this](./examples/people) for a complete example of how to generate the code
based on an existing struct.

See [this](./examples/via_parquet) for a complete example of how to generate the code
based on an existing parquet file.

## Supported Types 

The struct used to define the parquet data can have the following types:

```
int32
uint32
int64
uint64
float32
float64
string
bool
```

Each of these types may be a pointer to indicate that the data is optional.  The
struct can also embed another struct:

```go
type Being struct {
	ID  int32  `parquet:"id"`
	Age *int32 `parquet:"age"`
}

type Person struct {
	Being
	Username string `parquet:"username"`
}
```

Nested structs, however, are not supported at this time.  If you want a field to be
excluded from parquet you can tag it with a dash or make it private like so:

```go
type Being struct {
  	ID  int32  `parquet:"id"`
	Password string`parquet:"-"` //will not be written to parquet
	age int32                    //will not be written to parquet
}
```

## Parquetgen

Parquetgen is the command that go generate should call in
order to generate the code for your custom type:

```console
$ parquetgen --help
Usage of parquetgen:
  -ignore
    	ignore unsupported fields in -type, otherwise log.Fatal is called when an unsupported type is encountered (default true)
  -import string
    	import statement of -type if it doesn't live in -package
  -input string
    	path to the go file that defines -type
  -metadata
    	print the metadata of a parquet file (-parquet) and exit
  -output string
    	name of the file that is produced, defaults to parquet.go (default "parquet.go")
  -package string
    	package of the generated code
  -pageheaders
        print the page headers of a parquet file (-parquet) and exit (also prints the metadata)
  -parquet string
    	path to a parquet file (if you are generating code based on an existing parquet file)
  -struct-output string
    	name of the file that is produced, defaults to parquet.go (default "generated_struct.go")
  -type string
    	name of the struct that will used for writing and reading
```
