# Parquetgen example

To generate the code needed to run this example:

    cd cmd/parquetgen
    go get ./...
    go install
    cd ../../examples/people
    go generate

Go generate calls (see the top of main.go for the go:generate command):

    //parquetgen -input main.go -type Person -package main

which produces a file called parquet.go.  Now run:

    go run .

You should now have a parquet file that encodes a Person struct.  To
read the values back run:

    go run . -read people.parquet
