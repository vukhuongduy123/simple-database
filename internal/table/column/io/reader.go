package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	stdio "io"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
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
		if err == stdio.EOF {
			return buf.Len(), stdio.EOF
		}
		return 0, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}
	if dataType != datatype.TypeColumnDefinition {
		return buf.Len(), stdio.EOF
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
