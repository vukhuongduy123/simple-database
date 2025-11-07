package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
)

type Reader struct {
	reader io.Reader
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: reader}
}

func (r *Reader) Read(b []byte) (int, error) {
	if b == nil {
		return 0, platformerror.NewStackTraceError("Nil buffer given", platformerror.BinaryReadErrorCode)
	}
	n, err := r.reader.Read(b)
	if err != nil {
		return 0, err
	}
	if n != len(b) {
		return n, platformerror.NewStackTraceError(fmt.Sprintf("Expected %d, get %d", len(b), n), platformerror.IncompleteReadErrorCode)
	}
	return n, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	buf := make([]byte, datatype.LenInt32)
	if _, err := r.Read(buf); err != nil {
		return 0, platformerror.NewStackTraceError(err.Error(), platformerror.IncompleteReadErrorCode)
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (r *Reader) ReadByte() (byte, error) {
	buf := make([]byte, datatype.LenByte)
	if _, err := r.Read(buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *Reader) ReadTLV() ([]byte, error) {
	buf := bytes.Buffer{}
	dataType, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	buf.WriteByte(dataType)
	length, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}
	valBuf := make([]byte, length)
	if _, err := r.Read(valBuf); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}
	buf.Write(valBuf)
	return buf.Bytes(), nil
}
