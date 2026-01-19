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
	// CachePageKey is the key of the page that in the cache which need to be invalid
	CachePageKey string
	// Offset is the offset of the record in the file
	Offset uint32
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

func (r *RecordParser) Parse() (int32, error) {
	read := io.NewReader(r.file)
	r.reader = read

	startPos, err := r.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return -1, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}
	t, err := read.ReadByte()
	if err != nil {
		if err == stdio.EOF {
			endPos, err := r.file.Seek(0, stdio.SeekCurrent)
			if err != nil {
				return -1, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
			}
			return int32(endPos - startPos), stdio.EOF
		}
		return -1, err
	}

	// loop until we find a record
	for {
		if t == datatype.TypePage {
			// length of page which is not important
			_, err = read.ReadUint32()
			// type of next "thing"
			t, err = read.ReadByte()
		}

		if t != datatype.TypeRecord && t != datatype.TypeDeletedRecord {
			return -1, platformerror.NewStackTraceError(fmt.Sprintf("Expected %v or %v, got %v", datatype.TypeRecord,
				datatype.TypeDeletedRecord, t), platformerror.InvalidDataTypeErrorCode)
		}

		if t == datatype.TypeRecord {
			break
		}
		l, err := r.reader.ReadUint32()
		if err != nil {
			return -1, err
		}
		if _, err = r.file.Seek(int64(l), stdio.SeekCurrent); err != nil {
			return -1, err
		}
		t, err = read.ReadByte()
		if err != nil {
			return -1, err
		}
	}

	record := make(map[string]interface{})
	recordLength, err := read.ReadUint32()
	if err != nil {
		return -1, err
	}

	for i := 0; i < len(r.columns); i++ {
		tlvParser := parser.NewTLVParser(read)
		value, err := tlvParser.Parse()
		if errors.Is(err, stdio.EOF) {
			endPos, err := r.file.Seek(0, stdio.SeekCurrent)
			if err != nil {
				return -1, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
			}
			r.Value = NewRawRecord(recordLength, record)
			return int32(endPos - startPos), stdio.EOF
		}
		if err != nil {
			return -1, err
		}
		record[r.columns[i]] = value
	}
	endPos, err := r.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return -1, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}
	r.Value = NewRawRecord(recordLength, record)

	return int32(endPos - startPos), nil
}
