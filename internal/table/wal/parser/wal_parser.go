package parser

import (
	"bytes"
	"fmt"
	"simple-database/internal/platform/datatype"
	error2 "simple-database/internal/platform/error"
	"simple-database/internal/platform/parser"
)

type WALMarshaler struct {
	ID    string
	Table string
	Op    string
	Data  []byte
}

type WALLastCommitedMarshaler struct {
	ID  string
	Len uint32
}

type WALLastCommitedUnmarshaler struct {
	ID  string
	Len uint32
}

const (
	OpInsert = "insert"
)

func NewWALMarshaler(id, op, table string, data []byte) *WALMarshaler {
	return &WALMarshaler{
		ID:    id,
		Table: table,
		Op:    op,
		Data:  data,
	}
}

func NewWALLastCommitedMarshaler(id string, len uint32) *WALLastCommitedMarshaler {
	return &WALLastCommitedMarshaler{
		ID:  id,
		Len: len,
	}
}

func (m *WALLastCommitedMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	idMarshaler := parser.NewTLVMarshaler(m.ID)
	idBuf, err := idMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WALLastCommitedMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(idBuf)

	lengthMarshaler := parser.NewValueMarshaler(m.Len)
	lengthBuf, err := lengthMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WALLastCommitedMarshaler.MarshalBinary: %w", err)
	}

	buf.Write(lengthBuf)
	return buf.Bytes(), nil
}

//goland:noinspection DuplicatedCode
func (m *WALMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeMarshaler := parser.NewValueMarshaler(datatype.TypeWALEntry)
	typeBuf, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, error2.WrapError(fmt.Errorf("WALMarshaler.MarshalBinary: %w", err))
	}
	buf.Write(typeBuf)

	length, err := m.len()
	if err != nil {
		return nil, error2.WrapError(fmt.Errorf("WALMarshaler.MarshalBinary: %w", err))
	}
	lenMarshaler := parser.NewValueMarshaler(length)
	lenBuf, err := lenMarshaler.MarshalBinary()
	if err != nil {
		return nil, error2.WrapError(fmt.Errorf("WALMarshaler.MarshalBinary: %w", err))
	}
	buf.Write(lenBuf)

	idMarshaler := parser.NewTLVMarshaler(m.ID)
	idBuf, err := idMarshaler.MarshalBinary()
	if err != nil {
		return nil, error2.WrapError(fmt.Errorf("WALMarshaler.MarshalBinary: %w", err))
	}
	buf.Write(idBuf)

	tableMarshaler := parser.NewTLVMarshaler(m.Table)
	tableBuf, err := tableMarshaler.MarshalBinary()
	if err != nil {
		return nil, error2.WrapError(fmt.Errorf("WALMarshaler.MarshalBinary: %w", err))
	}
	buf.Write(tableBuf)

	opMarshaler := parser.NewTLVMarshaler(m.Op)
	opBuf, err := opMarshaler.MarshalBinary()
	if err != nil {
		return nil, error2.WrapError(fmt.Errorf("WALMarshaler.MarshalBinary: %w", err))
	}
	buf.Write(opBuf)

	buf.Write(m.Data)

	return buf.Bytes(), nil
}

func NewWALLastCommitedUnmarshaler() *WALLastCommitedUnmarshaler {
	return &WALLastCommitedUnmarshaler{}
}

func (u *WALLastCommitedUnmarshaler) UnmarshalBinary(data []byte) error {
	var bytesRead uint32 = 0

	byteUnmarshaler := parser.NewValueUnmarshaler[byte]()
	intUnmarshaler := parser.NewValueUnmarshaler[uint32]()
	strUnmarshaler := parser.NewValueUnmarshaler[string]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: type: %w", err)
	}
	bytesRead += datatype.LenByte

	// len
	if err := intUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: len: %w", err)
	}
	bytesRead += datatype.LenInt32

	// ID
	idUnmarshaler := parser.NewTLVUnmarshaler(strUnmarshaler)
	if err := idUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: ID: %w", err)
	}
	u.ID = idUnmarshaler.Value
	bytesRead += idUnmarshaler.BytesRead

	intUnmarshaler = parser.NewValueUnmarshaler[uint32]()
	lenUnmarshaler := parser.NewTLVUnmarshaler(intUnmarshaler)
	if err := lenUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: len of last record: %w", err)
	}
	u.Len = lenUnmarshaler.Value
	bytesRead += lenUnmarshaler.BytesRead

	return nil
}

func (m *WALMarshaler) len() (uint32, error) {
	idMarshaler := parser.NewTLVMarshaler(m.ID)
	opMarshaler := parser.NewTLVMarshaler(m.Op)
	tableMarshaler := parser.NewTLVMarshaler(m.Table)

	idLength, err := idMarshaler.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WALMarshaler.len: %w", err)
	}
	opLength, err := opMarshaler.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WALMarshaler.len: %w", err)
	}
	tableLength, err := tableMarshaler.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WALMarshaler.len: %w", err)
	}

	return idLength + opLength + tableLength + uint32(len(m.Data)), nil
}
