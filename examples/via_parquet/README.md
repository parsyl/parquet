# Parquetgen example (by reading a parquet file)

This example reads a parquet file and generates a struct,
a ParquetReader, and a ParquetWriter.

To generate the code needed to run this example:

```console
cd cmd/parquetgen
go get ./...
go install
cd ../../examples/via_parquet
go generate
```

Go generate calls (see the top of main.go for the go:generate command):

```console
parquetgen --parquet ./people.parquet --type Person --package main
```

Which produces two files: parquet.go and generated_struct.go.  Now run:

```console
go run .
```

To read the parquet file.  The generated ParquetWriter is not used in
this example (but could be used).  To see ParquetWriter in action see
the people example.
