package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	io2 "io"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/io"
)

type ColumnDefinitionReader struct {
	reader *io.Reader
}

func NewColumnDefinitionReader(reader *io.Reader) *ColumnDefinitionReader {
	return &ColumnDefinitionReader{
		reader: reader,
	}
}

func (r *ColumnDefinitionReader) Read(b []byte) (int, error) {
	buf := bytes.Buffer{}
	dataType, err := r.reader.ReadByte()
	if err != nil {
		if err == io2.EOF {
			return buf.Len(), io2.EOF
		}
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: data datatype: %w", err)
	}
	if dataType != datatype.TypeColumnDefinition {
		return buf.Len(), io2.EOF
	}
	buf.WriteByte(dataType)

	length, err := r.reader.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: len: %w", err)
	}
	if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: value: %w", err)
	}

	col := make([]byte, length)
	n, err := r.reader.Read(col)
	if err != nil {
		return n, fmt.Errorf("ColumnDefinitionReader.Read: reading file: %w", err)
	}
	buf.Write(col)

	copy(b, buf.Bytes())
	return buf.Len(), nil
}
