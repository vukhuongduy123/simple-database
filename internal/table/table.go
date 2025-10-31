package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	stdio "io"
	"os"
	"path/filepath"
	"simple-database/internal/platform/datatype"
	errors "simple-database/internal/platform/error"
	"simple-database/internal/platform/helper"
	io3 "simple-database/internal/platform/io"
	"simple-database/internal/platform/parser"
	"simple-database/internal/table/column"
	"simple-database/internal/table/column/io"
	platformparser "simple-database/internal/table/column/parser"
	"simple-database/internal/table/index"
	indexparser "simple-database/internal/table/index/parser"
	"simple-database/internal/table/wal"
	walparser "simple-database/internal/table/wal/parser"
	"slices"
	"strings"
)

type Columns map[string]*column.Column

const FileExtension = ".bin"
const PageSize = 128

type Table struct {
	Name            string
	file            *os.File
	ColumnNames     []string
	columns         Columns
	reader          *io3.Reader
	columnDefReader *io.ColumnDefinitionReader
	recordParser    *platformparser.RecordParser
	wal             *wal.WAL
	index           *index.Index
}

type DeletableRecord struct {
	offset int64
	len    uint32
}

func (t *Table) SetRecordParser(recParser *platformparser.RecordParser) error {
	if recParser == nil {
		return fmt.Errorf("Table.SetRecordParser: recParser cannot be nil")
	}
	t.recordParser = recParser
	return nil
}

func (t *Table) WriteColumnDefinitions() error {
	for _, c := range t.ColumnNames {
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

func NewTable(f *os.File, reader *io3.Reader, columnDefReader *io.ColumnDefinitionReader, parser *platformparser.RecordParser,
	wal *wal.WAL, index *index.Index) (*Table, error) {
	if f == nil || reader == nil || columnDefReader == nil {
		return nil, fmt.Errorf("NewTable: nil argument")
	}
	tableName, err := GetTableName(f)
	if err != nil {
		return nil, fmt.Errorf("NewTable: %w", err)
	}
	t := &Table{
		file:            f,
		Name:            tableName,
		columns:         make(Columns),
		reader:          reader,
		columnDefReader: columnDefReader,
		recordParser:    parser,
		wal:             wal,
		index:           index,
	}
	return t, nil
}

func NewTableWithColumns(file *os.File, columns Columns, columnNames []string, r *io3.Reader,
	columnDefReader *io.ColumnDefinitionReader, parser *platformparser.RecordParser, wal *wal.WAL, index *index.Index) (*Table, error) {
	return &Table{
		file:            file,
		ColumnNames:     columnNames,
		reader:          r,
		columns:         columns,
		columnDefReader: columnDefReader,
		recordParser:    parser,
		wal:             wal,
		index:           index,
	}, nil
}

func (t *Table) ReadColumnDefinitions() error {
	if _, err := t.file.Seek(0, stdio.SeekStart); err != nil {
		return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
	}

	for {
		buf := make([]byte, 1024)
		n, err := t.columnDefReader.Read(buf)
		if err != nil {
			if err == stdio.EOF {
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
		t.ColumnNames = append(t.ColumnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record map[string]any) (int, error) {
	if _, err := t.file.Seek(0, stdio.SeekEnd); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	if err := t.validateColumns(record); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	var sizeOfRecord uint32 = 0
	for _, col := range t.ColumnNames {
		val, ok := record[col]
		if !ok {
			return 0, errors.WrapError(fmt.Errorf("Table.Insert: missing column: %s", col))
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

	for _, col := range t.ColumnNames {
		v := record[col]
		tlvMarshaler := parser.NewTLVMarshaler(v)
		b, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		buf.Write(b)
	}

	walEntry, err := t.wal.Append(walparser.OpInsert, t.Name, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	page, err := t.insertIntoPage(buf)
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	if err = t.index.Add(record["id"].(int64), page.StartPos); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	err = t.wal.Commit(walEntry)
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
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

func (t *Table) getPrimaryKeyColumnName() string {
	var primaryKeyColumnName string
	for _, col := range t.columns {
		if col.IsPrimaryKey {
			return string(col.Name[:])
		}
	}

	return primaryKeyColumnName
}

func (t *Table) Select(whereClause map[string]interface{}) ([]map[string]interface{}, error) {
	if err := t.ensureFilePointer(); err != nil {
		return nil, errors.WrapError(fmt.Errorf("Table.Select: %w", err))
	}
	if err := t.validateWhereClause(whereClause); err != nil {
		return nil, errors.WrapError(fmt.Errorf("Table.Select: %w", err))
	}
	results := make([]map[string]interface{}, 0)
	fields := make([]string, 0)
	for k := range whereClause {
		fields = append(fields, k)
	}

	singleResult := false
	primaryKeyName := t.getPrimaryKeyColumnName()

	if slices.Contains(fields, primaryKeyName) {
		item, err := t.index.Get(whereClause[primaryKeyName].(int64))
		singleResult = true

		if err == nil {
			if _, err = t.file.Seek(item.PagePos, stdio.SeekStart); err != nil {
				return nil, fmt.Errorf("Table.Select: %w", err)
			}
			pr := indexparser.NewPageReader(t.reader)
			pageContent := make([]byte, PageSize+datatype.LenMeta)

			n, err := pr.Read(pageContent)
			if err != nil {
				return nil, fmt.Errorf("Table.Select: %w", err)
			}

			pageContent = pageContent[:n]
			reader := bytes.NewReader(pageContent)
			t.recordParser = platformparser.NewRecordParser(reader, t.ColumnNames)
		}
	}
	defer func() {
		t.recordParser = platformparser.NewRecordParser(t.file, t.ColumnNames)
	}()

	for {
		err := t.recordParser.Parse()
		if err == stdio.EOF {
			return results, nil
		}
		if err != nil {
			return nil, errors.WrapError(fmt.Errorf("Table.Select: %w", err))
		}
		rawRecord := t.recordParser.Value

		if err = t.ensureColumnLength(rawRecord.Record); err != nil {
			return nil, errors.WrapError(fmt.Errorf("Table.Select: %w", err))
		}
		if !t.evaluateWhereClause(whereClause, rawRecord.Record) {
			continue
		}

		results = append(results, rawRecord.Record)

		if singleResult {
			return results, nil
		}
	}
}

func (t *Table) ensureFilePointer() error {
	if _, err := t.file.Seek(0, stdio.SeekStart); err != nil {
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	if err := t.seekUntil(datatype.TypeRecord); err != nil {
		if err == stdio.EOF {
			return err
		}
		return fmt.Errorf("Table.ensureFilePointer: %w", err)
	}
	return nil
}

func (t *Table) seekUntil(targetType byte) error {
	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == stdio.EOF {
				return err
			}
			return fmt.Errorf("Table.seekUntil: readByte: %w", err)
		}
		if dataType == targetType {
			if _, err = t.file.Seek(-1*datatype.LenByte, stdio.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: %w", err)
			}
			return nil
		}

		if targetType == datatype.TypeRecord && dataType == datatype.TypePage {
			// Ignore page's len
			if _, err := t.reader.ReadUint32(); err != nil {
				return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
			}
			// The first type flag inside a page should be a record
			dataType, err = t.skipDeletedRecords()

			if err != nil {
				return fmt.Errorf("Table.seekUntil: readByte: %w", err)
			}
			if dataType != targetType {
				return fmt.Errorf("Table.seekUntil: first byte inside a page should be %d but %d found", datatype.TypeRecord, dataType)
			}
			if _, err = t.file.Seek(-1, stdio.SeekCurrent); err != nil {
				return fmt.Errorf("Table.seekUntil: file.Seek: %w", err)
			}
			return nil
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("Table.seekUntil: readUint32: %w", err)
		}

		if _, err = t.file.Seek(int64(length), stdio.SeekCurrent); err != nil {
			return fmt.Errorf("Table.seekUntil: %w", err)
		}
	}
}

func (t *Table) skipDeletedRecords() (dataType byte, err error) {
	for {
		dataType, err := t.reader.ReadByte()
		if err != nil {
			if err == stdio.EOF {
				return 0, err
			}
			return 0, fmt.Errorf("Table.skipDeletedRecords: %w", err)
		}
		if dataType == datatype.TypeDeletedRecord {
			l, err := t.reader.ReadUint32()
			if err != nil {
				return 0, fmt.Errorf("RecordParser.Parse: %w", err)
			}
			if _, err = t.file.Seek(int64(l), stdio.SeekCurrent); err != nil {
				return 0, fmt.Errorf("RecordParser.Parse: %w", err)
			}
		}
		if dataType == datatype.TypeRecord {
			return dataType, nil
		}
	}
}

func (t *Table) validateWhereClause(whereClause map[string]interface{}) error {
	if whereClause == nil {
		return nil
	}

	for k := range whereClause {
		if !slices.Contains(t.ColumnNames, k) {
			return fmt.Errorf("unknown column in where statement: %s", k)
		}
	}
	return nil
}

func (t *Table) evaluateWhereClause(whereClause map[string]interface{}, record map[string]interface{}) bool {
	if whereClause == nil {
		return true
	}

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
		if _, err := t.file.Seek(rec.offset, stdio.SeekStart); err != nil {
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
			if err == stdio.EOF {
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

		pos, err := t.file.Seek(0, stdio.SeekCurrent)
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
		if err == stdio.EOF {
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
		pos, err := t.file.Seek(0, stdio.SeekCurrent)
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

func GetTableName(f *os.File) (string, error) {
	// path/to/db/table.bin
	parts := strings.Split(f.Name(), ".")
	if len(parts) != 2 {
		return "", errors.NewInvalidFilename(f.Name())
	}
	filenameParts := strings.Split(parts[0], string(filepath.Separator))
	if len(filenameParts) == 0 {
		return "", errors.NewInvalidFilename(f.Name())
	}
	return filenameParts[len(filenameParts)-1], nil
}

func (t *Table) RestoreWAL() error {
	if _, err := t.file.Seek(0, stdio.SeekEnd); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	restorableData, err := t.wal.GetRestorableData()
	if err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	// Nothing to restore
	if restorableData == nil {
		fmt.Printf("RestoreWAL skipped\n")
		return nil
	}
	n, err := t.file.Write(restorableData.Data)
	if err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	if n != len(restorableData.Data) {
		return fmt.Errorf("Table.RestoreWAL: %w", errors.NewIncompleteWriteError(len(restorableData.Data), n))
	}
	fmt.Printf("RestoreWAL wrote %d bytes\n", n)
	if err = t.wal.Commit(restorableData.LastEntry); err != nil {
		return fmt.Errorf("Table.RestoreWAL: %w", err)
	}
	return nil
}

func (t *Table) seekToNextPage(lenToFit uint32) (*index.Page, error) {
	_, err := t.file.Seek(0, stdio.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Table.seekToNextPage: %w", err)
	}

	for {
		err = t.seekUntil(datatype.TypePage)
		if err != nil {
			if err == stdio.EOF {
				return t.insertEmptyPage()
			}

			return nil, fmt.Errorf("Table.seekToNextPage: %w", err)
		}

		// Skipping the type definition byte
		if _, err = t.reader.ReadByte(); err != nil {
			return nil, fmt.Errorf("Table.seekToNextPage: readByte: %w", err)
		}

		currPageLen, err := t.reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("Table.seekToNextPage: readUint32: %w", err)
		}

		if currPageLen+lenToFit <= PageSize {
			pagePos, err := t.file.Seek(-1*datatype.LenMeta, stdio.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("Table.seekToNextPage: file.Seek: %w", err)
			}

			_, err = t.file.Seek(int64(currPageLen)+datatype.LenMeta, stdio.SeekCurrent)
			return index.NewPage(pagePos), err
		}
	}

}

func (t *Table) insertEmptyPage() (*index.Page, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypePage); err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: type: %w", err)
	}
	// length
	if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: len: %w", err)
	}
	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: file.Write: %w", err)
	}
	if n != buf.Len() {
		return nil, errors.NewIncompleteWriteError(buf.Len(), n)
	}

	curPos, err := t.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("Table.insertEmptyPage: %w", err)
	}
	// startPos should point at the very first byte, that is types.TypePage and 5 bytes before the current pos
	startPos := curPos - (datatype.LenMeta)
	if startPos <= 0 {
		return nil, fmt.Errorf("Table.insertEmptyPage: unable to insert new page: start should be positive: %d", startPos)
	}
	return index.NewPage(startPos), nil
}

// insertIntoPage finds the first page that can fit buf and writes it into the page
func (t *Table) insertIntoPage(buf bytes.Buffer) (*index.Page, error) {
	page, err := t.seekToNextPage(uint32(buf.Len()))
	if err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: %w", err)
	}
	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: file.Write: %w", err)
	}
	if n != buf.Len() {
		return nil, errors.NewIncompleteWriteError(buf.Len(), n)
	}
	// seek back to the beginning of the page
	if _, err = t.file.Seek(page.StartPos, stdio.SeekStart); err != nil {
		return nil, fmt.Errorf("Table.insertIntoPage: file.Seek: %w", err)
	}
	return page, t.updatePageSize(page.StartPos, int32(buf.Len()))
}

// updatePageSize increases or decreases the size of a page by offset
// if the new size is 0, the page is removed
func (t *Table) updatePageSize(page int64, offset int32) (e error) {
	origPos, err := t.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	defer func() {
		_, err := t.file.Seek(origPos, stdio.SeekStart)
		e = err
	}()

	if _, err = t.file.Seek(page, stdio.SeekStart); err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}

	dataType, err := t.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	if dataType != datatype.TypePage {
		return fmt.Errorf("Table.updatePageSize: file pointer is at wrong position: expected: %d, actual: %d", datatype.TypePage, dataType)
	}
	length, err := t.reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}
	_, err = t.file.Seek(-1*datatype.LenInt32, stdio.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}

	var newLength uint32
	if offset >= 0 {
		newLength = length + uint32(offset)
	} else {
		newLength = length - uint32(-offset)
	}

	marshaler := parser.NewValueMarshaler[uint32](newLength)
	b, err := marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("Table.updatePageSize: %w", err)
	}

	n, err := t.file.Write(b)
	if n != len(b) {
		return errors.NewIncompleteWriteError(len(b), n)
	}

	if newLength == 0 {
		if err = t.removeEmptyPage(page); err != nil {
			return fmt.Errorf("Table.updatePageSize: %w", err)
		}
	}
	return nil
}

func (t *Table) removeEmptyPage(page int64) (e error) {
	origPos, err := t.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	defer func() {
		_, err := t.file.Seek(origPos, stdio.SeekStart)
		e = err
	}()

	if _, err = t.file.Seek(page, stdio.SeekStart); err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	dataType, err := t.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	if dataType != datatype.TypePage {
		return fmt.Errorf("Table.removePage: file pointer points to invalid byte: %d", dataType)
	}
	length, err := t.reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("Table.removePage: %w", err)
	}
	if length != 0 {
		return fmt.Errorf("Table.removePage: New page not empty %w", err)
	}
	stat, err := t.file.Stat()
	if err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}

	beforeReader := stdio.NewSectionReader(t.file, 0, page)
	afterReader := stdio.NewSectionReader(t.file, page+datatype.LenMeta, stat.Size())
	beforeBuf := make([]byte, page)

	if _, err = beforeReader.Read(beforeBuf); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}

	afterBuf := make([]byte, stat.Size()-(page+datatype.LenMeta))
	if _, err = afterReader.Read(afterBuf); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}

	if _, err = t.file.Seek(0, stdio.SeekStart); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}

	bw, err := t.file.Write(beforeBuf)
	if err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}

	aw, err := t.file.Write(afterBuf)
	if err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}

	if err = t.file.Truncate(int64(bw + aw)); err != nil {
		return fmt.Errorf("Table.removeEmptyPage: %w", err)
	}
	return nil
}
