package parser

import (
	"errors"
	"fmt"
	stdio "io"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/io"
	"simple-database/internal/platform/parser"
)

type RecordParser struct {
	file    stdio.ReadSeeker
	columns []string
	Value   *RawRecord
	reader  *io.Reader
}

// RawRecord represents one record read from the table file
// As the data is stored in TLV format, it stores the columns in a slice
type RawRecord struct {
	// Size is the size of the record in bytes. This only includes the actual fields
	Size uint32
	// FullSize is a sum of [RawRecord.Size] and [types.LenMeta] that includes metadata associated with records such as the type and length bytes
	FullSize uint32
	// Record contains the actual fields
	Record RecordValue
}

type RecordValue map[string]any

func NewRecordParser(f stdio.ReadSeeker, columns []string) *RecordParser {
	return &RecordParser{
		file:    f,
		columns: columns,
	}
}

func NewRawRecord(size uint32, record map[string]interface{}) *RawRecord {
	return &RawRecord{
		Size:     size,
		FullSize: size + datatype.LenMeta,
		Record:   record,
	}
}

func (r *RecordParser) Parse() error {
	read := io.NewReader(r.file)
	r.reader = read

	t, err := read.ReadByte()
	if err != nil {
		if err == stdio.EOF {
			return err
		}
		return err
	}

	if t == datatype.TypePage {
		// length of page which is not important
		_, err = read.ReadUint32()
		// type of next "thing"
		t, err = read.ReadByte()
	}

	if t != datatype.TypeRecord && t != datatype.TypeDeletedRecord {
		return platformerror.NewStackTraceError(fmt.Sprintf("Expected %v or %v, got %v", datatype.TypeRecord, datatype.TypeDeletedRecord,
			t), platformerror.InvalidDataTypeErrorCode)
	}

	// loop until we find a record
	for {
		if t == datatype.TypeRecord {
			break
		}
		l, err := r.reader.ReadUint32()
		if err != nil {
			return err
		}
		if _, err = r.file.Seek(int64(l), stdio.SeekCurrent); err != nil {
			return err
		}
		t, err = read.ReadByte()
		if err != nil {
			return err
		}
	}

	record := make(map[string]interface{})
	recordLength, err := read.ReadUint32()
	if err != nil {
		return err
	}

	for i := 0; i < len(r.columns); i++ {
		tlvParser := parser.NewTLVParser(read)
		value, err := tlvParser.Parse()
		if errors.Is(err, stdio.EOF) {
			r.Value = NewRawRecord(recordLength, record)
			return nil
		}
		if err != nil {
			return err
		}
		record[r.columns[i]] = value
	}
	r.Value = NewRawRecord(recordLength, record)

	return nil
}
