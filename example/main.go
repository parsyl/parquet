package main

import (
	"log"
	"os"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"

	p "github.com/cswank/parquet"
)

func main() {
	recs := []p.Record{}
	for i := 0; i < 2; i++ {
		var age *int32
		if i%2 == 0 {
			a := int32(20 + i%5)
			age = &a
		}
		recs = append(recs, p.Record{
			ID:  int32(i),
			Age: age,
		})
	}
	parq(recs)
	rec(recs)
}

func rec(records []p.Record) {
	f, err := os.Create("rec.parquet")
	if err != nil {
		log.Fatal(err)
	}

	rec := p.New(f)

	for _, r := range records {
		rec.Add(r)
	}

	if err := rec.Write(); err != nil {
		log.Fatal(err)
	}

	f.Close()
}

func parq(records []p.Record) {
	var err error
	fw, err := ParquetFile.NewLocalFileWriter("flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := ParquetWriter.NewParquetWriter(fw, new(p.Record), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	pw.NP = 1
	for _, rec := range records {
		if err = pw.Write(rec); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	///read
	// fr, err := ParquetFile.NewLocalFileReader("flat.parquet")
	// if err != nil {
	// 	log.Println("Can't open file")
	// 	return
	// }

	// pr, err := ParquetReader.NewParquetReader(fr, new(Student), 4)
	// if err != nil {
	// 	log.Println("Can't create parquet reader", err)
	// 	return
	// }
	// num = int(pr.GetNumRows())
	// for i := 0; i < num/10; i++ {
	// 	if i%2 == 0 {
	// 		pr.SkipRows(10) //skip 10 rows
	// 		continue
	// 	}
	// 	stus := make([]Student, 10) //read 10 rows
	// 	if err = pr.Read(&stus); err != nil {
	// 		log.Println("Read error", err)
	// 	}
	// 	log.Println(stus)
	// }

	// pr.ReadStop()
	// fr.Close()

}
