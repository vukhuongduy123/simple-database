package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
	platformio "simple-database/internal/platform/io"
)

type PageReader struct {
	reader *platformio.Reader
}

func NewPageReader(r *platformio.Reader) *PageReader {
	return &PageReader{
		reader: r,
	}
}

func (r *PageReader) Read(b []byte) (int, error) {
	// using the underlying reader to read type, length, and value of the page
	t, err := r.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("PageReaeer.Read: %w", err)
	}
	if t != datatype.TypePage {
		return 0, fmt.Errorf("PageReader.Read: type byte should be %d, found: %d", datatype.TypePage, t)
	}
	length, err := r.reader.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("PageReader.Read: %w", err)
	}

	val := make([]byte, length)
	n, err := r.reader.Read(val)
	if err != nil {
		return 0, fmt.Errorf("PageReader.Read: %w", err)
	}
	if n != int(length) {
		return 0, errors.NewIncompleteReadError(int(length), n)
	}

	// copy type, length, and value into a buffer
	buf := bytes.Buffer{}
	if err := binary.Write(&buf, binary.LittleEndian, t); err != nil {
		return 0, fmt.Errorf("PageReader.Read: len: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return 0, fmt.Errorf("PageReader.Read: type: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, val); err != nil {
		return 0, fmt.Errorf("PageReader.Read: val: %w", err)
	}

	copy(b, buf.Bytes())
	return buf.Len(), nil
}
