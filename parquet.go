package parquet

import (
	"encoding/binary"
	"io"
)

type PageType int64

const (
	PageType_DATA_PAGE       PageType = 0
	PageType_INDEX_PAGE      PageType = 1
	PageType_DICTIONARY_PAGE PageType = 2
	PageType_DATA_PAGE_V2    PageType = 3
)

type FieldRepetitionType int64

const (
	FieldRepetitionType_REQUIRED FieldRepetitionType = 0
	FieldRepetitionType_OPTIONAL FieldRepetitionType = 1
	FieldRepetitionType_REPEATED FieldRepetitionType = 2
)

type Type int64

const (
	Type_BOOLEAN              Type = 0
	Type_INT32                Type = 1
	Type_INT64                Type = 2
	Type_INT96                Type = 3
	Type_FLOAT                Type = 4
	Type_DOUBLE               Type = 5
	Type_BYTE_ARRAY           Type = 6
	Type_FIXED_LEN_BYTE_ARRAY Type = 7
)

type ConvertedType int64

const (
	ConvertedType_UTF8             ConvertedType = 0
	ConvertedType_MAP              ConvertedType = 1
	ConvertedType_MAP_KEY_VALUE    ConvertedType = 2
	ConvertedType_LIST             ConvertedType = 3
	ConvertedType_ENUM             ConvertedType = 4
	ConvertedType_DECIMAL          ConvertedType = 5
	ConvertedType_DATE             ConvertedType = 6
	ConvertedType_TIME_MILLIS      ConvertedType = 7
	ConvertedType_TIME_MICROS      ConvertedType = 8
	ConvertedType_TIMESTAMP_MILLIS ConvertedType = 9
	ConvertedType_TIMESTAMP_MICROS ConvertedType = 10
	ConvertedType_UINT_8           ConvertedType = 11
	ConvertedType_UINT_16          ConvertedType = 12
	ConvertedType_UINT_32          ConvertedType = 13
	ConvertedType_UINT_64          ConvertedType = 14
	ConvertedType_INT_8            ConvertedType = 15
	ConvertedType_INT_16           ConvertedType = 16
	ConvertedType_INT_32           ConvertedType = 17
	ConvertedType_INT_64           ConvertedType = 18
	ConvertedType_JSON             ConvertedType = 19
	ConvertedType_BSON             ConvertedType = 20
	ConvertedType_INTERVAL         ConvertedType = 21
)

type Encoding int64

const (
	Encoding_PLAIN                   Encoding = 0
	Encoding_PLAIN_DICTIONARY        Encoding = 2
	Encoding_RLE                     Encoding = 3
	Encoding_BIT_PACKED              Encoding = 4
	Encoding_DELTA_BINARY_PACKED     Encoding = 5
	Encoding_DELTA_LENGTH_BYTE_ARRAY Encoding = 6
	Encoding_DELTA_BYTE_ARRAY        Encoding = 7
	Encoding_RLE_DICTIONARY          Encoding = 8
)

type CompressionCodec int64

const (
	CompressionCodec_UNCOMPRESSED CompressionCodec = 0
	CompressionCodec_SNAPPY       CompressionCodec = 1
	CompressionCodec_GZIP         CompressionCodec = 2
	CompressionCodec_LZO          CompressionCodec = 3
)

type FileMetaData struct {
	Version          int32           `thrift:"version,1,required" db:"version" json:"version"`
	Schema           []SchemaElement `thrift:"schema,2,required" db:"schema" json:"schema"`
	NumRows          int64           `thrift:"num_rows,3,required" db:"num_rows" json:"num_rows"`
	RowGroups        []RowGroup      `thrift:"row_groups,4,required" db:"row_groups" json:"row_groups"`
	KeyValueMetadata []KeyValue      `thrift:"key_value_metadata,5" db:"key_value_metadata" json:"key_value_metadata,omitempty"`
	CreatedBy        *string         `thrift:"created_by,6" db:"created_by" json:"created_by,omitempty"`
}

type SchemaElement struct {
	Type           *Type                `thrift:"type,1" db:"type" json:"type,omitempty"`
	TypeLength     *int32               `thrift:"type_length,2" db:"type_length" json:"type_length,omitempty"`
	RepetitionType *FieldRepetitionType `thrift:"repetition_type,3" db:"repetition_type" json:"repetition_type,omitempty"`
	Name           string               `thrift:"name,4,required" db:"name" json:"name"`
	NumChildren    *int32               `thrift:"num_children,5" db:"num_children" json:"num_children,omitempty"`
	ConvertedType  *ConvertedType       `thrift:"converted_type,6" db:"converted_type" json:"converted_type,omitempty"`
	Scale          *int32               `thrift:"scale,7" db:"scale" json:"scale,omitempty"`
	Precision      *int32               `thrift:"precision,8" db:"precision" json:"precision,omitempty"`
	FieldID        *int32               `thrift:"field_id,9" db:"field_id" json:"field_id,omitempty"`
}

type RowGroup struct {
	Columns        []ColumnChunk   `thrift:"columns,1,required" db:"columns" json:"columns"`
	TotalByteSize  int64           `thrift:"total_byte_size,2,required" db:"total_byte_size" json:"total_byte_size"`
	NumRows        int64           `thrift:"num_rows,3,required" db:"num_rows" json:"num_rows"`
	SortingColumns []SortingColumn `thrift:"sorting_columns,4" db:"sorting_columns" json:"sorting_columns,omitempty"`
}

type ColumnChunk struct {
	FilePath   *string         `thrift:"file_path,1" db:"file_path" json:"file_path,omitempty"`
	FileOffset int64           `thrift:"file_offset,2,required" db:"file_offset" json:"file_offset"`
	MetaData   *ColumnMetaData `thrift:"meta_data,3" db:"meta_data" json:"meta_data,omitempty"`
}

type SortingColumn struct {
	ColumnIdx  int32 `thrift:"column_idx,1,required" db:"column_idx" json:"column_idx"`
	Descending bool  `thrift:"descending,2,required" db:"descending" json:"descending"`
	NullsFirst bool  `thrift:"nulls_first,3,required" db:"nulls_first" json:"nulls_first"`
}

type PageHeader struct {
	Type                 PageType        `thrift:"type,1,required" db:"type" json:"type"`
	UncompressedPageSize int32           `thrift:"uncompressed_page_size,2,required" db:"uncompressed_page_size" json:"uncompressed_page_size"`
	CompressedPageSize   int32           `thrift:"compressed_page_size,3,required" db:"compressed_page_size" json:"compressed_page_size"`
	Crc                  *int32          `thrift:"crc,4" db:"crc" json:"crc,omitempty"`
	DataPageHeader       *DataPageHeader `thrift:"data_page_header,5" db:"data_page_header" json:"data_page_header,omitempty"`
}

func (p PageHeader) Write(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, int64(p.Type)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.UncompressedPageSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.CompressedPageSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, p.DataPageHeader.NumValues); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int32(p.DataPageHeader.Encoding)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int32(p.DataPageHeader.DefinitionLevelEncoding)); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, int32(p.DataPageHeader.RepetitionLevelEncoding))
}

func int64ToZigzag(l int64) int64 {
	return (l << 1) ^ (l >> 63)
}

func int32ToZigzag(n int32) int32 {
	return (n << 1) ^ (n >> 31)
}

type IndexPageHeader struct {
}

type DataPageHeader struct {
	NumValues               int32       `thrift:"num_values,1,required" db:"num_values" json:"num_values"`
	Encoding                Encoding    `thrift:"encoding,2,required" db:"encoding" json:"encoding"`
	DefinitionLevelEncoding Encoding    `thrift:"definition_level_encoding,3,required" db:"definition_level_encoding" json:"definition_level_encoding"`
	RepetitionLevelEncoding Encoding    `thrift:"repetition_level_encoding,4,required" db:"repetition_level_encoding" json:"repetition_level_encoding"`
	Statistics              *Statistics `thrift:"statistics,5" db:"statistics" json:"statistics,omitempty"`
}

type ColumnMetaData struct {
	Type                  Type                 `thrift:"type,1,required" db:"type" json:"type"`
	Encodings             []Encoding           `thrift:"encodings,2,required" db:"encodings" json:"encodings"`
	PathInSchema          []string             `thrift:"path_in_schema,3,required" db:"path_in_schema" json:"path_in_schema"`
	Codec                 CompressionCodec     `thrift:"codec,4,required" db:"codec" json:"codec"`
	NumValues             int64                `thrift:"num_values,5,required" db:"num_values" json:"num_values"`
	TotalUncompressedSize int64                `thrift:"total_uncompressed_size,6,required" db:"total_uncompressed_size" json:"total_uncompressed_size"`
	TotalCompressedSize   int64                `thrift:"total_compressed_size,7,required" db:"total_compressed_size" json:"total_compressed_size"`
	KeyValueMetadata      []*KeyValue          `thrift:"key_value_metadata,8" db:"key_value_metadata" json:"key_value_metadata,omitempty"`
	DataPageOffset        int64                `thrift:"data_page_offset,9,required" db:"data_page_offset" json:"data_page_offset"`
	IndexPageOffset       *int64               `thrift:"index_page_offset,10" db:"index_page_offset" json:"index_page_offset,omitempty"`
	DictionaryPageOffset  *int64               `thrift:"dictionary_page_offset,11" db:"dictionary_page_offset" json:"dictionary_page_offset,omitempty"`
	Statistics            *Statistics          `thrift:"statistics,12" db:"statistics" json:"statistics,omitempty"`
	EncodingStats         []*PageEncodingStats `thrift:"encoding_stats,13" db:"encoding_stats" json:"encoding_stats,omitempty"`
}

type Statistics struct {
	Max           []byte `thrift:"max,1" db:"max" json:"max,omitempty"`
	Min           []byte `thrift:"min,2" db:"min" json:"min,omitempty"`
	NullCount     *int64 `thrift:"null_count,3" db:"null_count" json:"null_count,omitempty"`
	DistinctCount *int64 `thrift:"distinct_count,4" db:"distinct_count" json:"distinct_count,omitempty"`
}

type KeyValue struct {
	Key   string  `thrift:"key,1,required" db:"key" json:"key"`
	Value *string `thrift:"value,2" db:"value" json:"value,omitempty"`
}

type PageEncodingStats struct {
	PageType PageType `thrift:"page_type,1,required" db:"page_type" json:"page_type"`
	Encoding Encoding `thrift:"encoding,2,required" db:"encoding" json:"encoding"`
	Count    int32    `thrift:"count,3,required" db:"count" json:"count"`
}
