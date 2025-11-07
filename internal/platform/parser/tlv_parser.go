package parser

import (
	"fmt"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/io"
)

type TLVParser struct {
	reader *io.Reader
}

func NewTLVParser(reader *io.Reader) *TLVParser {
	return &TLVParser{
		reader: reader,
	}
}

func (p *TLVParser) Parse() (interface{}, error) {
	data, err := p.reader.ReadTLV()
	if err != nil {
		return nil, err
	}

	switch data[0] {
	case datatype.TypeInt64:
		return unmarshalValue[int64](data)
	case datatype.TypeInt32:
		return unmarshalValue[int32](data)
	case datatype.TypeByte:
		return unmarshalValue[byte](data)
	case datatype.TypeBool:
		return unmarshalValue[bool](data)
	case datatype.TypeString:
		return unmarshalValue[string](data)
	}
	return nil, platformerror.NewStackTraceError(fmt.Sprintf("TLVParser.Parse: unknown type: %d", data[0]), platformerror.UnknownDatatypeErrorCode)
}

func unmarshalValue[T any](data []byte) (interface{}, error) {
	tlvUnmarshaler := NewTLVUnmarshaler(NewValueUnmarshaler[T]())
	if err := tlvUnmarshaler.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return tlvUnmarshaler.Value, nil
}
