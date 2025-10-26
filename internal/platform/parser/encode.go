package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
)

type ValueMarshaler[T any] struct {
	Value T
}

func (m *ValueMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	switch v := any(m.Value).(type) {
	case string:
		if err := binary.Write(&buf, binary.LittleEndian, []byte(v)); err != nil {
			return nil, fmt.Errorf("ValueMarshaler.MarshalBinary: %w", err)
		}
	default:
		if err := binary.Write(&buf, binary.LittleEndian, m.Value); err != nil {
			return nil, fmt.Errorf("ValueMarshaler.MarshalBinary: %w", err)
		}
	}
	return buf.Bytes(), nil
}

type ValueUnmarshaler[T any] struct {
	Value T
}

func (u *ValueUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	var value T
	switch v := any(&value).(type) {
	case *string:
		*v = string(data)
	default:
		err := binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &value)
		if err != nil {
			return fmt.Errorf("ValueUnmarshaler.UnmarshalBinary: %w", err)
		}
	}
	u.Value = value
	return nil
}

type TLVMarshaler[T any] struct {
	Value          T
	ValueMarshaler *ValueMarshaler[T]
}

func (m *TLVMarshaler[T]) dataLength() (uint32, error) {
	switch v := any(m.Value).(type) {
	case byte:
		return 1, nil
	case int32:
		return 4, nil
	case int64:
		return 8, nil
	case bool:
		return 1, nil
	case string:
		return uint32(len(v)), nil
	default:
		return 0, &errors.UnsupportedDataTypeError{DataType: fmt.Sprintf("%T", v)}
	}
}

func (m *TLVMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeFlag, err := m.typeFlag()
	if err != nil {
		return nil, err
	}
	length, err := m.dataLength()
	if err != nil {
		return nil, err
	}
	// datatype
	if err := binary.Write(&buf, binary.LittleEndian, typeFlag); err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	// length
	if err := binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	valueBuf, err := m.ValueMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(valueBuf)
	return buf.Bytes(), nil
}

func (m *TLVMarshaler[T]) typeFlag() (byte, error) {
	switch v := any(m.Value).(type) {
	case byte:
		return datatype.TypeByte, nil
	case int32:
		return datatype.TypeInt32, nil
	case int64:
		return datatype.TypeInt64, nil
	case bool:
		return datatype.TypeBool, nil
	case string:
		return datatype.TypeString, nil
	default:
		return 0, &errors.UnsupportedDataTypeError{DataType: fmt.Sprintf("%T", v)}
	}
}

func (m *TLVMarshaler[T]) TLVLength() (uint32, error) {
	switch v := any(m.Value).(type) {
	case byte:
		return datatype.LenMeta + datatype.LenByte, nil
	case int32, uint32:
		return datatype.LenMeta + datatype.LenInt32, nil
	case int64:
		return datatype.LenMeta + datatype.LenInt64, nil
	case bool:
		return datatype.LenMeta + datatype.LenByte, nil
	case string:
		return datatype.LenMeta + uint32(len(v)), nil
	default:
		return 0, &errors.UnsupportedDataTypeError{DataType: fmt.Sprintf("%T", v)}
	}
}

type TLVUnmarshaler[T any] struct {
	dataType    byte
	length      uint32
	Value       T
	unmarshaler *ValueUnmarshaler[T]
	BytesRead   uint32
}

func NewValueUnmarshaler[T any]() *ValueUnmarshaler[T] {
	return &ValueUnmarshaler[T]{}
}

func NewValueMarshaler[T any](val T) *ValueMarshaler[T] {
	return &ValueMarshaler[T]{
		Value: val,
	}
}

func (u *TLVUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	u.BytesRead = 0
	byteUnmarshaler := NewValueUnmarshaler[byte]()
	intUnmarshaler := NewValueUnmarshaler[uint32]()
	// datatype
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.dataType = byteUnmarshaler.Value
	u.BytesRead += datatype.LenByte
	// length
	if err := intUnmarshaler.UnmarshalBinary(data[u.BytesRead:]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.length = intUnmarshaler.Value
	u.BytesRead += datatype.LenInt32
	// value
	if err := u.unmarshaler.UnmarshalBinary(data[u.BytesRead:]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.Value = u.unmarshaler.Value
	u.BytesRead += u.length
	return nil
}

func NewTLVMarshaler[T any](val T) *TLVMarshaler[T] {
	return &TLVMarshaler[T]{
		Value:          val,
		ValueMarshaler: NewValueMarshaler(val),
	}
}

func NewTLVUnmarshaler[T any](unmarshaler *ValueUnmarshaler[T]) *TLVUnmarshaler[T] {
	return &TLVUnmarshaler[T]{
		unmarshaler: unmarshaler,
	}
}
