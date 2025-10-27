package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/parser"
)

type ColumnDefinitionMarshaler struct {
	Name         [64]byte
	DataType     byte
	AllowNull    bool
	IsPrimaryKey bool
}

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return datatype.LenByte + // datatype of col name
		datatype.LenInt32 + // len of col name
		uint32(len(c.Name)) + // value of col name
		datatype.LenByte + // datatype of data datatype
		datatype.LenInt32 + // len of data datatype
		uint32(binary.Size(c.DataType)) + // value of data datatype
		datatype.LenByte + // datatype of allow_null
		datatype.LenInt32 + // len of allow_null
		uint32(binary.Size(c.AllowNull)) + // value of allow_null
		datatype.LenByte + // datatype of is_primary_key
		datatype.LenInt32 + // len of is_primary_key
		uint32(binary.Size(c.IsPrimaryKey)) // value of is_primary_key
}

func (c *ColumnDefinitionMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeFlag := parser.NewValueMarshaler[byte](datatype.TypeColumnDefinition)
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

	isPrimaryKey := parser.NewTLVMarshaler[bool](c.IsPrimaryKey)
	b, err = isPrimaryKey.MarshalBinary()
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

	if err := byteUnmarshalBinary.UnmarshalBinary(data[readBytes : readBytes+datatype.LenByte]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}

	typeFlag := byteUnmarshalBinary.Value
	if typeFlag != datatype.TypeColumnDefinition {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: not column datatype %b", typeFlag)
	}

	readBytes += datatype.LenByte

	if err := sizeUnmarshalBinary.UnmarshalBinary(data[readBytes : readBytes+datatype.LenInt32]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}

	readBytes += datatype.LenInt32

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
	readBytes += dataTypeUnmarshaler.BytesRead
	dataType := dataTypeUnmarshaler.Value

	allowNullUnmarshaler := parser.NewTLVUnmarshaler[byte](byteUnmarshalBinary)
	if err := allowNullUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	readBytes += allowNullUnmarshaler.BytesRead
	allowNull := allowNullUnmarshaler.Value

	isPrimaryKeyUnmarshaler := parser.NewTLVUnmarshaler[byte](byteUnmarshalBinary)
	if err := isPrimaryKeyUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: %w", err)
	}
	readBytes += isPrimaryKeyUnmarshaler.BytesRead
	isPrimaryKey := isPrimaryKeyUnmarshaler.Value

	copy(c.Name[:], name)
	c.DataType = dataType
	c.AllowNull = allowNull != 0
	c.IsPrimaryKey = isPrimaryKey != 0

	return nil
}

func NewColumnDefinitionMarshaler(name [64]byte, dataType byte, isPrimaryKey bool, allowNull bool) *ColumnDefinitionMarshaler {
	return &ColumnDefinitionMarshaler{
		Name:         name,
		DataType:     dataType,
		IsPrimaryKey: isPrimaryKey,
		AllowNull:    allowNull,
	}
}
