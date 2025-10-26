package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	io2 "io"
	"os"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
	"simple-database/internal/platform/helper"
	io3 "simple-database/internal/platform/io"
	"simple-database/internal/platform/parser"
	"simple-database/internal/table/column"
	"simple-database/internal/table/column/io"
	platformparser "simple-database/internal/table/column/parser"
	"slices"
)

type Columns map[string]*column.Column

const FileExtension = ".bin"

type Table struct {
	Name            string
	file            *os.File
	columnNames     []string
	columns         Columns
	reader          *io3.Reader
	columnDefReader *io.ColumnDefinitionReader
	recordParser    *platformparser.RecordParser
}

type DeletableRecord struct {
	offset int64
	len    uint32
}

func (t *Table) WriteColumnDefinitions() error {
	for _, c := range t.columnNames {
		b, err := t.columns[c].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
		colWriter := io.NewColumnDefinitionWriter(t.file)
		if _, err = colWriter.Write(b); err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
	}
	return nil
}

func NewTable(file *os.File, reader *io3.Reader, columnDefReader *io.ColumnDefinitionReader, parser *platformparser.RecordParser) (*Table, error) {
	return &Table{
		file:            file,
		reader:          reader,
		columnDefReader: columnDefReader,
		recordParser:    parser,
	}, nil
}

func NewTableWithColumns(file *os.File, columns Columns, columnNames []string, r *io3.Reader,
	columnDefReader *io.ColumnDefinitionReader, parser *platformparser.RecordParser) (*Table, error) {
	return &Table{
		file:            file,
		columnNames:     columnNames,
		reader:          r,
		columns:         columns,
		columnDefReader: columnDefReader,
		recordParser:    parser,
	}, nil
}

func (t *Table) ReadColumnDefinitions() error {
	if _, err := t.file.Seek(0, io2.SeekStart); err != nil {
		return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
	}

	for {
		buf := make([]byte, 1024)
		n, err := t.columnDefReader.Read(buf)
		if err != nil {
			if err == io2.EOF {
				break
			}
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		col := column.Column{}
		if err = col.UnmarshalBinary(buf[:n]); err != nil {
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		colName := helper.ToString(col.Name[:])
		t.columns[colName] = &col
		t.columnNames = append(t.columnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record map[string]any) (int, error) {
	if _, err := t.file.Seek(0, io2.SeekEnd); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	if err := t.validateColumns(record); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	var sizeOfRecord uint32 = 0
	for _, col := range t.columnNames {
		val, ok := record[col]
		if !ok {
			return 0, fmt.Errorf("Table.Insert: missing column: %s", col)
		}
		tlvMarshaler := parser.NewTLVMarshaler(val)
		length, err := tlvMarshaler.TLVLength()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		sizeOfRecord += length
	}

	buf := bytes.Buffer{}
	byteMarshaler := parser.NewValueMarshaler(datatype.TypeRecord)
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(typeBuf)

	intMarshaler := parser.NewValueMarshaler(sizeOfRecord)
	lenBuf, err := intMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(lenBuf)

	for _, col := range t.columnNames {
		v := record[col]
		tlvMarshaler := parser.NewTLVMarshaler(v)
		b, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		buf.Write(b)
	}
	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if n != buf.Len() {
		return 0, errors.NewIncompleteWriteError(n, buf.Len())
	}
	return 1, nil
}

func (t *Table) validateColumns(record map[string]any) error {
	for col, val := range record {
		if _, ok := t.columns[col]; !ok {
			return fmt.Errorf("Table.validateColumns: %w", errors.NewUnknownColumnError(col))
		}
		if !t.columns[col].Opts.AllowNull && val == nil {
			return fmt.Errorf("Table.validateColumns: %w", errors.NewColumnNotNullableError(col))
		}
	}
	return nil
}

func (t *Table) Select(whereClause map[string]interface{}) ([]map[string]interface{}, error) {
	if err := t.ensureFilePointer(); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}
	if err := t.validateWhereClause(whereClause); err != nil {
		return nil, fmt.Errorf("Table.Select: %w", err)
	}
	results := make([]map[string]interface{}, 0)

	for {
		err := t.recordParser.Parse()
		if err == io2.EOF {
			return results, nil
		}
		if err != nil {
			return nil, fmt.Errorf("Table.Select: %w", err)
		}
		rawRecord := t.recordParser.Value

		if err = t.ensureColumnLength(rawRecord.Record); err != nil {
			return nil, fmt.Errorf("Table.Select: %w", err)
		}
		if !t.evaluateWhereClause(whereClause, rawRecord.Record) {
			continue
		}

		results = append(results, rawRecord.Record)
	}
}

func (t *Table) ensureFilePointer() error {
	if _, err := t.file.Seek(0, io2.SeekStart); err != nil {
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	if err := t.seekUntil(datatype.TypeRecord); err != nil {
		if err == io2.EOF {
			return nil
		}
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	return nil
}

func (t *Table) seekUntil(targetType byte) error {
	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == io2.EOF {
				return err
			}
			return fmt.Errorf("Table.seekUntil: readByte: %w", err)
		}
		if dataType == targetType {
			if _, err = t.file.Seek(-1*datatype.LenByte, io2.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: %w", err)
			}
			return nil
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
		}

		if _, err = t.file.Seek(int64(length), io2.SeekCurrent); err != nil {
			return fmt.Errorf("Table.seekUntil: %w", err)
		}
	}
}

func (t *Table) validateWhereClause(whereClause map[string]interface{}) error {
	for k := range whereClause {
		if !slices.Contains(t.columnNames, k) {
			return fmt.Errorf("unknown column in where statement: %s", k)
		}
	}
	return nil
}

func (t *Table) evaluateWhereClause(whereClause map[string]interface{}, record map[string]interface{}) bool {
	for k, v := range whereClause {
		if record[k] != v {
			return false
		}
	}
	return true
}

func (t *Table) ensureColumnLength(record map[string]interface{}) error {
	if len(record) != len(t.columns) {
		return errors.NewMismatchingColumnsError(len(t.columns), len(record))
	}
	return nil
}
func (t *Table) markRecordsAsDeleted(deletableRecords []*DeletableRecord) (n int, e error) {
	for _, rec := range deletableRecords {
		if _, err := t.file.Seek(rec.offset, io2.SeekStart); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
		err := binary.Write(t.file, binary.LittleEndian, datatype.TypeDeletedRecord)
		if err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}

		zeroBytes := make([]byte, length)
		if err = binary.Write(t.file, binary.LittleEndian, zeroBytes); err != nil {
			return 0, fmt.Errorf("Table.markRecordsDeleted: %w", err)
		}
	}
	return len(deletableRecords), nil
}

func newDeletableRecord(offset int64, len uint32) *DeletableRecord {
	return &DeletableRecord{
		offset: offset,
		len:    len,
	}
}

func (t *Table) Delete(whereClause map[string]interface{}) (int, error) {
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}
	if err := t.validateWhereClause(whereClause); err != nil {
		return 0, fmt.Errorf("Table.Delete: %w", err)
	}
	deletableRecords := make([]*DeletableRecord, 0)
	for {
		if err := t.recordParser.Parse(); err != nil {
			if err == io2.EOF {
				break
			}
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}

		rawRecord := t.recordParser.Value
		if err := t.ensureColumnLength(rawRecord.Record); err != nil {
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}
		if !t.evaluateWhereClause(whereClause, rawRecord.Record) {
			continue
		}

		pos, err := t.file.Seek(0, io2.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.Delete: %w", err)
		}

		deletableRecords = append(deletableRecords, newDeletableRecord(pos-int64(rawRecord.FullSize), rawRecord.FullSize))
	}
	return t.markRecordsAsDeleted(deletableRecords)
}

func (t *Table) Update(whereClause map[string]interface{}, values map[string]interface{}) (int, error) {
	if err := t.validateColumns(values); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}
	if err := t.ensureFilePointer(); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	deletableRecords := make([]*DeletableRecord, 0)
	rawRecords := make([]*platformparser.RawRecord, 0)
	for {
		err := t.recordParser.Parse()
		if err == io2.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
		rawRecord := t.recordParser.Value

		if err := t.ensureColumnLength(rawRecord.Record); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}

		if !t.evaluateWhereClause(whereClause, rawRecord.Record) {
			continue
		}

		rawRecords = append(rawRecords, rawRecord)
		pos, err := t.file.Seek(0, io2.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
		deletableRecords = append(deletableRecords, newDeletableRecord(pos-int64(rawRecord.FullSize), rawRecord.FullSize))
	}

	if _, err := t.markRecordsAsDeleted(deletableRecords); err != nil {
		return 0, fmt.Errorf("Table.Update: %w", err)
	}

	for _, rawRecord := range rawRecords {
		updatedRecord := make(map[string]interface{})
		for col, v := range rawRecord.Record {
			if updatedVal, ok := values[col]; ok {
				updatedRecord[col] = updatedVal
			} else {
				updatedRecord[col] = v
			}
		}
		if _, err := t.Insert(updatedRecord); err != nil {
			return 0, fmt.Errorf("Table.Update: %w", err)
		}
	}
	return len(rawRecords), nil
}
