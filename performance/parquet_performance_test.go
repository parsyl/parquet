package performance

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/bxcodec/faker/v3"
	"github.com/parsyl/parquet/performance/base"
	"github.com/parsyl/parquet/performance/message"
)

const (
	writeBatch = 5_000
	inputSize  = 100_000
)

type parquetWriter interface {
	Add(rec message.Message)
	Write() error
	Close() error
}

func generateTestData(count int) []message.Message {
	res := make([]message.Message, count)
	for i := 0; i < count; i++ {
		err := faker.FakeData(&res[i])
		// faker doesn't set nil, so we set them ourselves sometimes
		if rand.Intn(2) == 0 {
			res[i].ColBool0 = nil
			res[i].ColFloat0 = nil
			res[i].ColFloat32_0 = nil
			res[i].ColInt0 = nil
			res[i].ColInt32_0 = nil
			res[i].ColStr0 = nil
		}
		if err != nil {
			panic(err)
		}
	}
	return res
}

func benchmarkParquet(b *testing.B, data []message.Message, buf *bytes.Buffer, getWriter func(*bytes.Buffer) parquetWriter) {
	writeOnce := func() {
		writer := getWriter(buf)
		for i := range data {
			writer.Add(data[i])
			if i%writeBatch == 0 {
				err := writer.Write()
				if err != nil {
					b.Fatalf(err.Error())
				}
			}
		}
		err := writer.Write()
		if err != nil {
			b.Fatalf(err.Error())
		}
		err = writer.Close()
		if err != nil {
			b.Fatalf(err.Error())
		}
	}

	writeOnce() // the first time will allocate the buffer to the correct size
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		writeOnce()
	}
}

func BenchmarkWrite(b *testing.B) {
	data := generateTestData(inputSize)

	const mib = 1024 * 1024 * 1

	var baseBuff bytes.Buffer
	baseBuff.Grow(mib)
	b.Run("base", func(b *testing.B) {
		getWriter := func(buf *bytes.Buffer) parquetWriter {
			writer, err := base.NewParquetWriter(&baseBuff)
			if err != nil {
				b.Fatal(err)
			}
			return writer
		}

		benchmarkParquet(b, data, &baseBuff, getWriter)
	})

	var optBuff bytes.Buffer
	optBuff.Grow(mib)
	b.Run("opt", func(b *testing.B) {
		getWriter := func(buf *bytes.Buffer) parquetWriter {
			writer, err := NewParquetWriter(&optBuff)
			if err != nil {
				b.Fatal(err)
			}
			return writer
		}
		benchmarkParquet(b, data, &optBuff, getWriter)
	})

	baseBytes := baseBuff.Bytes()
	optBytes := optBuff.Bytes()

	// to make sure we didn't break anything
	if len(baseBytes) != len(optBytes) || len(baseBytes) == 0 {
		b.Fatal("length", baseBuff.Len(), optBuff.Len())
	}

	for i := 0; i < len(baseBytes); i++ {
		if baseBytes[i] != optBytes[i] {
			b.Fatal("bytes incorrect at ", i)
		}
	}
}
