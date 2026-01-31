package parser

import (
	"fmt"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/io"
)

type TLVParser struct {
	reader    *io.Reader
	bytesRead uint32
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
		dataRead, bytesRead, e := unmarshalValue[int64](data)
		p.bytesRead = bytesRead
		return dataRead, e
	case datatype.TypeInt32:
		dataRead, bytesRead, e := unmarshalValue[int32](data)
		p.bytesRead = bytesRead
		return dataRead, e
	case datatype.TypeFloat64:
		dataRead, bytesRead, e := unmarshalValue[float64](data)
		p.bytesRead = bytesRead
		return dataRead, e
	case datatype.TypeFloat32:
		dataRead, bytesRead, e := unmarshalValue[float32](data)
		p.bytesRead = bytesRead
		return dataRead, e
	case datatype.TypeByte:
		dataRead, bytesRead, e := unmarshalValue[byte](data)
		p.bytesRead = bytesRead
		return dataRead, e
	case datatype.TypeBool:
		dataRead, bytesRead, e := unmarshalValue[bool](data)
		p.bytesRead = bytesRead
		return dataRead, e
	case datatype.TypeString:
		dataRead, bytesRead, e := unmarshalValue[string](data)
		p.bytesRead = bytesRead
		return dataRead, e
	}
	return nil, platformerror.NewStackTraceError(fmt.Sprintf("TLVParser.Parse: unknown type: %d", data[0]), platformerror.UnknownDatatypeErrorCode)
}

func (p *TLVParser) BytesRead() uint32 {
	return p.bytesRead
}

func unmarshalValue[T any](data []byte) (interface{}, uint32, error) {
	tlvUnmarshaler := NewTLVUnmarshaler(NewValueUnmarshaler[T]())
	if err := tlvUnmarshaler.UnmarshalBinary(data); err != nil {
		return nil, 0, err
	}
	return tlvUnmarshaler.Value, tlvUnmarshaler.BytesRead, nil
}
