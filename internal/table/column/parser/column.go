package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"simple-database/internal/platform/parser"
)

type ColumnDefinitionMarshaler struct {
	Name      [64]byte
	DataType  byte
	AllowNull bool
}

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return parser.LenByte + // type of col name
		parser.LenInt32 + // len of col name
		uint32(len(c.Name)) + // value of col name
		parser.LenByte + // type of data type
		parser.LenInt32 + // len of data type
		uint32(binary.Size(c.DataType)) + // value of data type
		parser.LenByte + // type of allow_null
		parser.LenInt32 + // len of allow_null
		uint32(binary.Size(c.AllowNull)) // value of allow_null
}

func (c *ColumnDefinitionMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeFlag := parser.NewValueMarshaler[byte](parser.TypeColumnDefinition)
	b, err := typeFlag.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	length := parser.NewValueMarshaler[uint32](c.Size())
	b, err = length.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	name := parser.NewTLVMarshaler[string](string(c.Name[:]))
	b, err = name.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	dataType := parser.NewTLVMarshaler[byte](c.DataType)
	b, err = dataType.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	allowNull := parser.NewTLVMarshaler[bool](c.AllowNull)
	b, err = allowNull.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(b)

	return buf.Bytes(), nil
}

func (c *ColumnDefinitionMarshaler) UnmarshalBinary(data []byte) error {
	var readBytes uint32

	byteUnmarshalBinary := parser.NewValueUnmarshaler[byte]()
	sizeUnmarshalBinary := parser.NewValueUnmarshaler[uint32]()

	if err := byteUnmarshalBinary.UnmarshalBinary(data[readBytes : readBytes+parser.LenByte]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}

	typeFlag := byteUnmarshalBinary.Value
	if typeFlag != parser.TypeColumnDefinition {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: not column type %b", typeFlag)
	}

	readBytes += parser.LenByte

	if err := sizeUnmarshalBinary.UnmarshalBinary(data[readBytes : readBytes+parser.LenInt32]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}

	readBytes += parser.LenInt32

	nameUnmarshaler := parser.NewTLVUnmarshaler[string](parser.NewValueUnmarshaler[string]())
	if err := nameUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	name := nameUnmarshaler.Value

	readBytes += nameUnmarshaler.BytesRead

	dataTypeUnmarshaler := parser.NewTLVUnmarshaler[byte](byteUnmarshalBinary)
	if err := dataTypeUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	readBytes += nameUnmarshaler.BytesRead
	dataType := dataTypeUnmarshaler.Value

	allowNullUnmarshaler := parser.NewTLVUnmarshaler[byte](byteUnmarshalBinary)
	if err := allowNullUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	readBytes += allowNullUnmarshaler.BytesRead
	allowNull := allowNullUnmarshaler.Value

	copy(c.Name[:], name)
	c.DataType = dataType
	c.AllowNull = allowNull != 0

	return nil
}

func NewColumnDefinitionMarshaler(name [64]byte, dataType byte, allowNull bool) *ColumnDefinitionMarshaler {
	return &ColumnDefinitionMarshaler{
		Name:      name,
		DataType:  dataType,
		AllowNull: allowNull,
	}
}
