# Parquet

Parquet generates a parquet reader and writer for a single struct.

## Installation

    go get -u github.com/parsyl/parquet/...

## Usage

First define a struct for the data to be written to parquet:

	type Person struct {
      	ID  int32  `parquet:"id"`
		Age *int32 `parquet:"age"`
	}

Next, add a go:generate comment somewhere (in this example all code lives
in main.go):

    //go:generate parquetgen -input main.go -type Person -package main

Generate the code for the reader and writer:

    go generate

A new file (parquet.go) has now been written that defines ParquetWriter
and ParquetReader.  Next, make use of the writer and reader:


	package main
    
    func main() {
    	f, err := os.Create("people.parquet")
    	if err != nil {
    		log.Fatal(err)
    	}    	

		//MaxPageSize optionally defines the number of rows in each column chunk (default is 1000)
    	w, err := NewParquetWriter(f, MaxPageSize(10000))
    	if err != nil {
    		log.Fatal(err)
    	}

    	w.Add(Person{ID:1, Age: getAge(30)})
		w.Add(Person{ID:2})

		//Each call to write creates a new parquet row group.
    	if err := w.Write(); err != nil {
    		log.Fatal(err)
    	}

    	if err := w.Close(); err != nil {
    		log.Fatal(err)
    	}

		f.Close()

		//now read the data back
    	f, err = os.Open("people.parquet")
    	if err != nil {
    		log.Fatal(err)
    	}
    	defer f.Close()

    	r, err := NewParquetReader(f)
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

	func getAge(a int32) *int32 {return &a}

See [this](./examples/people) for a complete example of how to use it.

## Supported Types

The struct used to define the parquet data can have the following types:

    int32
    uint32
    int64
    uint64
    float32
    float64
    string
    bool

Each of these types may be a pointer to indicate that the data is optional.  The
struct can also embed another struct:

	type Being struct {
		ID  int32  `parquet:"id"`
		Age *int32 `parquet:"age"`
	}

	type Person struct {
    	Being
		Username string `parquet:"username"`
	}

Nested structs, however, are not supported at this time.  If you want a field to be
excluded from parquet you can tag it with a dash or make it private like so:

	type Being struct {
      	ID  int32  `parquet:"id"`
		Password string`parquet:"-"` //will not be written to parquet
		age int32                    //will not be written to parquet
	}

