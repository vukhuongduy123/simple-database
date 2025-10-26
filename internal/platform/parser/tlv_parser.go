package parser

import (
	"fmt"
	"simple-database/internal/platform/datatype"
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
		return fmt.Errorf("TLVParser.Parse: %w", err), nil
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
	return nil, fmt.Errorf("TLVParser.Parse: unknown type: %d", data[0])
}

func unmarshalValue[T any](data []byte) (interface{}, error) {
	tlvUnmarshaler := NewTLVUnmarshaler(NewValueUnmarshaler[T]())
	if err := tlvUnmarshaler.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("parser.unmarshalValue: %w", err)
	}
	return tlvUnmarshaler.Value, nil
}
