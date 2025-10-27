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
	typeMarshaler := parser.NewValueMarshaler(datatype.TypeWALItem)
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
