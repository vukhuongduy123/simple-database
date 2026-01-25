package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/parser"
)

type ColumnDefinitionMarshaler struct {
	Name     [64]byte
	DataType byte
	Opts     int32
}

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return datatype.LenByte + // datatype of col name
		datatype.LenInt32 + // len of col name
		uint32(len(c.Name)) + // value of col name
		datatype.LenByte + // datatype of data datatype
		datatype.LenInt32 + // len of data datatype
		uint32(binary.Size(c.DataType)) + // value of data datatype
		datatype.LenByte + // datatype of opts
		datatype.LenInt32 + // len of opts
		uint32(binary.Size(c.Opts))
}

func (c *ColumnDefinitionMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeFlag := parser.NewValueMarshaler[byte](datatype.TypeColumnDefinition)
	b, err := typeFlag.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(b)

	length := parser.NewValueMarshaler[uint32](c.Size())
	b, err = length.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(b)

	name := parser.NewTLVMarshaler[string](string(c.Name[:]))
	b, err = name.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(b)

	dataType := parser.NewTLVMarshaler[byte](c.DataType)
	b, err = dataType.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(b)

	opts := parser.NewTLVMarshaler[int32](c.Opts)
	b, err = opts.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(b)

	return buf.Bytes(), nil
}

func (c *ColumnDefinitionMarshaler) UnmarshalBinary(data []byte) error {
	var readBytes uint32

	byteUnmarshalBinary := parser.NewValueUnmarshaler[byte]()
	uint32UnmarshalBinary := parser.NewValueUnmarshaler[uint32]()
	int32UnmarshalBinary := parser.NewValueUnmarshaler[int32]()

	if err := byteUnmarshalBinary.UnmarshalBinary(data[readBytes : readBytes+datatype.LenByte]); err != nil {
		return err
	}

	typeFlag := byteUnmarshalBinary.Value
	if typeFlag != datatype.TypeColumnDefinition {
		return platformerror.NewStackTraceError(fmt.Sprintf("Expected %v, got %v", datatype.TypeColumnDefinition, typeFlag),
			platformerror.InvalidDataTypeErrorCode)
	}
	readBytes += datatype.LenByte

	if err := uint32UnmarshalBinary.UnmarshalBinary(data[readBytes : readBytes+datatype.LenInt32]); err != nil {
		return err
	}
	readBytes += datatype.LenInt32

	nameUnmarshaler := parser.NewTLVUnmarshaler[string](parser.NewValueUnmarshaler[string]())
	if err := nameUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return err
	}
	name := nameUnmarshaler.Value
	readBytes += nameUnmarshaler.BytesRead

	dataTypeUnmarshaler := parser.NewTLVUnmarshaler[byte](byteUnmarshalBinary)
	if err := dataTypeUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return err
	}
	readBytes += dataTypeUnmarshaler.BytesRead
	dataType := dataTypeUnmarshaler.Value

	optsUnmarshaler := parser.NewTLVUnmarshaler[int32](int32UnmarshalBinary)
	if err := optsUnmarshaler.UnmarshalBinary(data[readBytes:]); err != nil {
		return err
	}
	readBytes += optsUnmarshaler.BytesRead
	opts := optsUnmarshaler.Value

	copy(c.Name[:], name)
	c.DataType = dataType
	c.Opts = opts

	return nil
}

func NewColumnDefinitionMarshaler(name [64]byte, dataType byte, opts int32) *ColumnDefinitionMarshaler {
	return &ColumnDefinitionMarshaler{
		Name:     name,
		DataType: dataType,
		Opts:     opts,
	}
}
