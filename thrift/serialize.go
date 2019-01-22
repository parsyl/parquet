package thrift

import (
	"bytes"
	"context"
	"io"

	th "github.com/apache/thrift/lib/go/thrift"
)

type Thrift struct {
	ts *th.TSerializer
}

func New() *Thrift {
	ts := th.NewTSerializer()
	ts.Protocol = th.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	return &Thrift{ts: ts}
}

func (t *Thrift) PageHeader(w io.Writer, ps, cps, l int32) error {
	ph := &pageHeader{
		UncompressedPageSize: ps,
		CompressedPageSize:   cps,
		DataPageHeader: &dataPageHeader{
			NumValues:               l,
			DefinitionLevelEncoding: Encoding_PLAIN,
			RepetitionLevelEncoding: Encoding_PLAIN,
		},
	}

	buf, err := t.ts.Write(context.TODO(), ph)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewBuffer(buf))
	return err
}
