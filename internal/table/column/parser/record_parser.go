package parser

import (
	"errors"
	"fmt"
	io2 "io"
	"os"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/io"
	"simple-database/internal/platform/parser"
)

type RecordParser struct {
	file    *os.File
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
	Record map[string]interface{}
}

func NewRecordParser(f *os.File, columns []string) *RecordParser {
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

func (r *RecordParser) skipDeletedRecords() error {
	for {
		t, err := r.reader.ReadByte()
		if err != nil {
			if err == io2.EOF {
				return err
			}
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		if t == datatype.TypeDeletedRecord {
			l, err := r.reader.ReadUint32()
			if err != nil {
				return fmt.Errorf("RecordParser.Parse: %w", err)
			}
			if _, err = r.file.Seek(int64(l), io2.SeekCurrent); err != nil {
				return fmt.Errorf("RecordParser.Parse: %w", err)
			}
		}
		if t == datatype.TypeRecord {
			return nil
		}
	}
}

func (r *RecordParser) Parse() error {
	read := io.NewReader(r.file)

	t, err := read.ReadByte()
	if err != nil {
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}
	if t != datatype.TypeRecord && t != datatype.TypeDeletedRecord {
		return fmt.Errorf(
			"RecordParser.Parse: file offset needs to point at a record definition",
		)
	}

	if t == datatype.TypeDeletedRecord {
		if _, err := r.file.Seek(-1*datatype.LenByte, io2.SeekCurrent); err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		err = r.skipDeletedRecords()
		if err != nil {
			if err == io2.EOF {
				return err
			}
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
	}

	record := make(map[string]interface{})
	recordLength, err := read.ReadUint32()
	if err != nil {
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}

	for i := 0; i < len(r.columns); i++ {
		tlvParser := parser.NewTLVParser(read)
		value, err := tlvParser.Parse()
		if errors.Is(err, io2.EOF) {
			r.Value = NewRawRecord(recordLength, record)
			return nil
		}
		if err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		record[r.columns[i]] = value
	}
	r.Value = NewRawRecord(recordLength, record)

	return nil
}
