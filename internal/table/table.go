package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	stdio "io"
	"os"
	"path/filepath"
	"simple-database/internal/platform"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/helper"
	platformio "simple-database/internal/platform/io"
	"simple-database/internal/platform/parser"
	"simple-database/internal/table/column"
	"simple-database/internal/table/column/io"
	tableparser "simple-database/internal/table/column/parser"
	"simple-database/internal/table/index"
	indexparser "simple-database/internal/table/index/parser"
	"slices"
	"strings"
)

type Columns map[string]*column.Column

const FileExtension = ".bin"
const PageSize = 4096 * 4

var lastPagePos int64 = -1
var pageRegionPos int64 = -1

type Table struct {
	Name            string
	file            *os.File
	ColumnNames     []string
	columns         Columns
	reader          *platformio.Reader
	columnDefReader *io.ColumnDefinitionReader
	recordParser    *tableparser.RecordParser
	indexes         map[string]*index.Index
	lru             *platform.LRU[string, index.Page]
}

type SelectResult struct {
	Rows          []tableparser.RawRecord
	AccessType    string
	RowsInspected int
	Extra         string
	pagePos       []int64
}

type Comparator struct {
	Operator string
	Value    any
}

type SelectCommand struct {
	WhereClause map[string]Comparator
	// TODO handle limit
	Limit uint
}

func (c *SelectCommand) FilteredColumnNames() []string {
	columnNames := make([]string, 0)
	for k := range c.WhereClause {
		columnNames = append(columnNames, k)
	}

	return columnNames
}

type DeleteResult struct {
	DeletedRecords []*tableparser.RawRecord
	AffectedPages  []*index.Page
}

type DeletableRecord struct {
	id     any
	offset int64
}

const AccessTypeAll = "All"
const AccessTypeIndex = "Index"

func newSelectResult() *SelectResult {
	return &SelectResult{
		AccessType: AccessTypeAll,
		Extra:      "Not using page cache",
		Rows:       make([]tableparser.RawRecord, 0),
	}
}

func (t *Table) writeColumnDefinitions() error {
	for _, c := range t.ColumnNames {
		b, err := t.columns[c].MarshalBinary()
		if err != nil {
			return err
		}
		colWriter := io.NewColumnDefinitionWriter(t.file)
		if _, err = colWriter.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func newTable(f *os.File) (*Table, error) {
	if f == nil {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Null file pointer"), platformerror.OpenFileErrorCode)
	}
	tableName, err := GetTableName(f)
	if err != nil {
		return nil, err
	}

	r := platformio.NewReader(f)
	columnDefReader := io.NewColumnDefinitionReader(r)

	return &Table{
		file:            f,
		Name:            tableName,
		columns:         make(Columns),
		reader:          r,
		columnDefReader: columnDefReader,
		lru:             platform.NewLRU[string, index.Page](10),
	}, nil
}

func (t *Table) initIndexes() {
	indexes := make(map[string]*index.Index)

	dbPath := GetPath(t.file)
	tableName, _ := GetTableName(t.file)

	for _, col := range t.columns {
		if col.Is(column.UsingIndex) {
			idxName := dbPath + "_" + tableName + "_" + helper.ToString(col.Name[:]) + "_idx.bin"
			idx := index.NewIndex(idxName, col.Is(column.UsingUniqueIndex))
			indexes[helper.ToString(col.Name[:])] = idx
		}
	}
	t.indexes = indexes
}

func NewTable(f *os.File) (*Table, error) {
	t, err := newTable(f)

	if err != nil {
		return nil, err
	}

	err = t.readColumnDefinitions()
	if err != nil {
		return nil, err
	}

	t.recordParser = tableparser.NewRecordParser(f, t.ColumnNames)
	t.initIndexes()

	return t, nil
}

func NewTableWithColumns(file *os.File, columns Columns) (*Table, error) {
	table, err := newTable(file)
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, 0)
	for _, col := range columns {
		columnNames = append(columnNames, helper.ToString(col.Name[:]))
	}

	table.ColumnNames = columnNames
	table.columns = columns

	table.initIndexes()
	err = table.writeColumnDefinitions()
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (t *Table) readColumnDefinitions() error {
	if _, err := t.file.Seek(0, stdio.SeekStart); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	for {
		buf := make([]byte, 1024)
		n, err := t.columnDefReader.Read(buf)
		if err != nil {
			if err == stdio.EOF {
				break
			}
			return err
		}
		col := column.Column{}
		if err = col.UnmarshalBinary(buf[:n]); err != nil {
			return err
		}
		colName := helper.ToString(col.Name[:])
		t.columns[colName] = &col
		t.ColumnNames = append(t.ColumnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record tableparser.RecordValue) (int, error) {
	if _, err := t.file.Seek(0, stdio.SeekEnd); err != nil {
		return 0, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	if err := t.validateColumns(record); err != nil {
		return 0, err
	}

	uniqueColumns := t.getUniqueColumns()
	if len(uniqueColumns) != 0 {
		for key, val := range record {
			_, ok := uniqueColumns[key]
			if !ok {
				continue
			}

			pages, err := t.indexes[key].Get(val, datatype.OperatorEqual)
			if err != nil {
				return 0, err
			}

			if pages != nil {
				return 0, platformerror.NewStackTraceError(fmt.Sprintf("Value %v already exist for unique index on column %v", val, key),
					platformerror.UniqueIndexViolationErrorCode)
			}
		}
	}

	var sizeOfRecord uint32 = 0
	for _, col := range t.ColumnNames {
		val, ok := record[col]
		if !ok {
			return 0, platformerror.NewStackTraceError(fmt.Sprintf("Table.Insert: missing column: %s", col), platformerror.MissingColumnErrorCode)
		}
		tlvMarshaler := parser.NewTLVMarshaler(val)
		length, err := tlvMarshaler.TLVLength()
		if err != nil {
			return 0, err
		}
		sizeOfRecord += length
	}

	buf := bytes.Buffer{}

	byteMarshaler := parser.NewValueMarshaler(datatype.TypeRecord)
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return 0, err
	}
	buf.Write(typeBuf)

	intMarshaler := parser.NewValueMarshaler(sizeOfRecord)
	lenBuf, err := intMarshaler.MarshalBinary()
	if err != nil {
		return 0, err
	}
	buf.Write(lenBuf)

	for _, col := range t.ColumnNames {
		v := record[col]
		tlvMarshaler := parser.NewTLVMarshaler(v)
		b, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return 0, err
		}
		buf.Write(b)
	}

	page, err := t.insertIntoPage(buf)
	if err != nil {
		return 0, err
	}

	primaryKeyColumnName := t.getPrimaryKeyColumnName()
	for k, v := range t.indexes {
		if err = v.Add(record[k], record[primaryKeyColumnName], page.StartPos); err != nil {
			return 0, err
		}
	}

	t.invalidateCache(page)

	return 1, nil
}

func (t *Table) validateColumns(record tableparser.RecordValue) error {
	for col, val := range record {
		if _, ok := t.columns[col]; !ok {
			return platformerror.NewStackTraceError(fmt.Sprintf("Unknown coloum: %s", col),
				platformerror.ColumnViolationErrorCode)
		}

		if !datatype.IsScalar(val) {
			return platformerror.NewStackTraceError(fmt.Sprintf("Column %s type %v not valid", col, val),
				platformerror.ColumnViolationErrorCode)
		}
	}
	return nil
}

func (t *Table) getUniqueColumns() Columns {
	uniqueColumns := make(Columns)
	for _, col := range t.columns {
		if col.Is(column.UsingUniqueIndex) {
			uniqueColumns[helper.ToString(col.Name[:])] = col
		}
	}
	return uniqueColumns
}

func (t *Table) getPrimaryKeyColumnName() string {
	var primaryKeyColumnName string
	for _, col := range t.columns {
		if col.Is(column.PrimaryKey) {
			return helper.ToString(col.Name[:])
		}
	}

	return primaryKeyColumnName
}

func (t *Table) getColumnsUsingIndex(filteredColumnNames []string) (string, bool) {
	for _, v := range filteredColumnNames {
		_, ok := t.indexes[v]
		if ok {
			return v, true
		}
	}

	return "", false
}

func (t *Table) Select(command SelectCommand) (*SelectResult, error) {
	if err := t.moveToFirstPageRegion(); err != nil {
		return nil, err
	}

	filteredColumnNames := command.FilteredColumnNames()
	if err := t.validateColumnNames(filteredColumnNames); err != nil {
		return nil, err
	}

	selectResult := newSelectResult()

	columnsUsingIndex, ok := t.getColumnsUsingIndex(filteredColumnNames)
	var indexKeys []index.Item

	if ok {
		selectResult.AccessType = AccessTypeIndex

		colVal := command.WhereClause[columnsUsingIndex].Value
		op := command.WhereClause[columnsUsingIndex].Operator

		keys, err := t.indexes[columnsUsingIndex].Get(colVal, op)
		if err != nil {
			return nil, err
		}
		if keys == nil {
			return selectResult, nil
		}

		indexKeys = keys
	}

	defer func() {
		t.recordParser = tableparser.NewRecordParser(t.file, t.ColumnNames)
	}()

	if selectResult.AccessType == AccessTypeIndex {
		for _, key := range indexKeys {
			pageEndPos := key.PagePos + PageSize
			pageKey := t.pageKey(key.PagePos)

			_, err := t.file.Seek(key.PagePos, stdio.SeekStart)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
			}

			if !t.lru.Contains(pageKey) {
				selectResult.Extra = "Not using page cache"
				pr := indexparser.NewPageReader(t.reader)
				pageContent := make([]byte, PageSize+datatype.LenMeta)
				n, err := pr.Read(pageContent)
				if err != nil {
					return nil, err
				}
				pageContent = pageContent[:n]
				reader := bytes.NewReader(pageContent)
				t.recordParser = tableparser.NewRecordParser(reader, t.ColumnNames)
				if err = t.lru.Put(pageKey, *index.NewPageWithContent(key.PagePos, pageContent)); err != nil {
					return nil, err
				}
			} else {
				page := t.lru.Get(pageKey)
				selectResult.Extra = "Using page cache"
				t.recordParser = tableparser.NewRecordParser(bytes.NewReader(page.Content), t.ColumnNames)
			}

			for {
				err := t.recordParser.Parse()
				if err != nil {
					if err == stdio.EOF {
						return selectResult, nil
					}
					return nil, err
				}

				curPos, err := t.file.Seek(0, stdio.SeekCurrent)
				if err != nil {
					return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
				}
				if curPos >= pageEndPos {
					continue
				}

				rawRecord := t.recordParser.Value
				selectResult.RowsInspected++

				if !t.evaluateWhereClause(command, rawRecord.Record) {
					continue
				}

				selectResult.Rows = append(selectResult.Rows, *rawRecord)
			}
		}
	} else {
		for {
			err := t.recordParser.Parse()
			if err != nil {
				if err == stdio.EOF {
					return selectResult, nil
				}
				return nil, err
			}

			rawRecord := t.recordParser.Value
			selectResult.RowsInspected++

			if !t.evaluateWhereClause(command, rawRecord.Record) {
				continue
			}

			selectResult.Rows = append(selectResult.Rows, *rawRecord)
		}
	}

	return selectResult, nil
}

func (t *Table) moveToFirstPageRegion() error {
	if pageRegionPos == -1 {
		if _, err := t.file.Seek(0, stdio.SeekStart); err != nil {
			return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
		}
		if err := t.seekUntil(datatype.TypePage); err != nil {
			return err
		}
		pageRegionPos, _ = t.file.Seek(0, stdio.SeekCurrent)
	} else {
		if _, err := t.file.Seek(pageRegionPos, stdio.SeekStart); err != nil {
			return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
		}
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
			return err
		}
		if dataType == targetType {
			if _, err = t.file.Seek(-1*datatype.LenByte, stdio.SeekCurrent); err != nil {
				return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
			}
			return nil
		}

		if targetType == datatype.TypeRecord && dataType == datatype.TypePage {
			// Ignore page's len
			if _, err := t.reader.ReadUint32(); err != nil {
				return err
			}
			// The first type flag inside a page should be a record
			dataType, err = t.skipDeletedRecords()

			if err != nil {
				return err
			}
			if dataType != targetType {
				return platformerror.NewStackTraceError(fmt.Sprintf("First byte inside a page should be %d but %d found",
					datatype.TypeRecord, dataType), platformerror.InvalidDataTypeErrorCode)
			}
			if _, err = t.file.Seek(-1, stdio.SeekCurrent); err != nil {
				return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
			}
			return nil
		}

		length, err := t.reader.ReadUint32()
		if err != nil {
			return err
		}

		if _, err = t.file.Seek(int64(length), stdio.SeekCurrent); err != nil {
			return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
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
			return 0, err
		}
		if dataType == datatype.TypeDeletedRecord {
			l, err := t.reader.ReadUint32()
			if err != nil {
				return 0, err
			}
			if _, err = t.file.Seek(int64(l), stdio.SeekCurrent); err != nil {
				return 0, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
			}
		}
		if dataType == datatype.TypeRecord {
			return dataType, nil
		}
	}
}

func (t *Table) validateColumnNames(columnNames []string) error {
	if columnNames == nil || len(columnNames) == 0 {
		return nil
	}

	for _, val := range columnNames {
		if !slices.Contains(t.ColumnNames, val) {
			return platformerror.NewStackTraceError(fmt.Sprintf("unknown column in where statement: %s", val),
				platformerror.ColumnViolationErrorCode)
		}
	}
	return nil
}

func (t *Table) evaluateWhereClause(command SelectCommand, record tableparser.RecordValue) bool {
	if command.WhereClause == nil || len(command.WhereClause) == 0 {
		return true
	}

	for k, v := range command.WhereClause {
		if !datatype.Compare(record[k], v.Value, v.Operator) {
			return false
		}
	}
	return true
}

func (t *Table) ensureColumnLength(record tableparser.RecordValue) error {
	if len(record) != len(t.columns) {
		return platformerror.NewStackTraceError(fmt.Sprintf("Expected column length: %d, got %d", len(record), len(t.columns)),
			platformerror.ColumnViolationErrorCode)
	}
	return nil
}

func (t *Table) markRecordsAsDeleted(deletableRecords []*DeletableRecord) (n int, e error) {
	for _, rec := range deletableRecords {
		if _, err := t.file.Seek(rec.offset, stdio.SeekStart); err != nil {
			return 0, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
		}
		err := binary.Write(t.file, binary.LittleEndian, datatype.TypeDeletedRecord)
		if err != nil {
			return 0, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
		}
	}
	return len(deletableRecords), nil
}

func newDeletableRecord(id any, offset int64) *DeletableRecord {
	return &DeletableRecord{
		id:     id,
		offset: offset,
	}
}

func newDeleteResult() *DeleteResult {
	return &DeleteResult{}
}

func (t *Table) Delete(command SelectCommand) (*DeleteResult, error) {
	if err := t.moveToFirstPageRegion(); err != nil {
		return nil, err
	}
	if err := t.validateColumnNames(command.FilteredColumnNames()); err != nil {
		return nil, err
	}
	deletableRecords := make([]*DeletableRecord, 0)
	primaryKeyColumnName := t.getPrimaryKeyColumnName()

	deleteResult := newDeleteResult()

	selectResult, err := t.Select(command)
	if err != nil {
		return nil, err
	}

	for _, row := range selectResult.Rows {
		id, _ := row.Record[primaryKeyColumnName]

		keys, err := t.indexes[primaryKeyColumnName].Get(id, datatype.OperatorEqual)
		if err != nil {
			return nil, err
		}

		for _, key := range keys {

			if !t.evaluateWhereClause(command, row.Record) {
				continue
			}

			pos, err := t.file.Seek(key.PagePos, stdio.SeekStart)
			if err != nil {
				return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
			}

			deletableRecord := newDeletableRecord(id, pos-int64(row.FullSize))
			deletableRecords = append(deletableRecords, deletableRecord)

			deleteResult.DeletedRecords = append(deleteResult.DeletedRecords, &row)

			deleteResult.AffectedPages = append(deleteResult.AffectedPages, index.NewPage(key.PagePos))
		}
	}

	if _, err := t.markRecordsAsDeleted(deletableRecords); err != nil {
		return nil, err
	}

	for _, rec := range deleteResult.DeletedRecords {
		for k, v := range rec.Record {
			idx, ok := t.indexes[k]
			if !ok {
				continue
			}

			if err := idx.Remove(v, rec.Record[primaryKeyColumnName]); err != nil {
				return nil, err
			}
		}
	}

	for _, p := range deleteResult.AffectedPages {
		t.invalidateCache(p)
	}

	return deleteResult, nil
}

func (t *Table) Update(command SelectCommand, record tableparser.RecordValue) (int, error) {
	if err := t.validateColumns(record); err != nil {
		return 0, err
	}

	deleteResult, err := t.Delete(command)
	if err != nil {
		return 0, err
	}

	rawRecords := deleteResult.DeletedRecords

	for _, rawRecord := range rawRecords {
		updatedRecord := make(map[string]interface{})
		for k, v := range rawRecord.Record {
			if updatedVal, ok := record[k]; ok {
				updatedRecord[k] = updatedVal
			} else {
				updatedRecord[k] = v
			}
		}
		if _, err = t.Insert(updatedRecord); err != nil {
			return 0, err
		}
	}
	return len(rawRecords), nil
}

func GetTableName(f *os.File) (string, error) {
	// path/to/db/table.bin
	parts := strings.Split(f.Name(), ".")
	if len(parts) != 2 {
		return "", platformerror.NewStackTraceError(fmt.Sprintf("Invalid table name: %s", f.Name()), platformerror.InvalidTableName)
	}
	filenameParts := strings.Split(parts[0], string(filepath.Separator))
	if len(filenameParts) == 0 {
		return "", platformerror.NewStackTraceError(fmt.Sprintf("Invalid table name: %s", f.Name()), platformerror.InvalidTableName)
	}
	return filenameParts[len(filenameParts)-1], nil
}

func GetPath(f *os.File) string {
	return filepath.Dir(f.Name()) + string(filepath.Separator)
}

func (t *Table) seekToNextPage(lenToFit uint32) (*index.Page, error) {
	_, err := t.file.Seek(0, stdio.SeekStart)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	if lastPagePos == -1 {
		err = t.seekUntil(datatype.TypePage)
		if err != nil {
			if err == stdio.EOF {
				page, err := t.insertEmptyPage()
				if err != nil {
					return nil, err
				}
				lastPagePos = page.StartPos
				return page, nil
			}

			return nil, err
		} else {
			lastPagePos, err = t.file.Seek(0, stdio.SeekCurrent)
		}
	} else {
		_, err = t.file.Seek(lastPagePos, stdio.SeekStart)
		if err != nil {
			return nil, err
		}
	}

	// Skipping the type definition byte
	if _, err = t.reader.ReadByte(); err != nil {
		return nil, err
	}

	currPageLen, err := t.reader.ReadUint32()
	if err != nil {
		return nil, err
	}

	if currPageLen+lenToFit <= PageSize {
		pagePos, err := t.file.Seek(-1*datatype.LenMeta, stdio.SeekCurrent)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
		}

		_, err = t.file.Seek(int64(currPageLen)+datatype.LenMeta, stdio.SeekCurrent)
		lastPagePos = pagePos
		return index.NewPage(pagePos), err
	} else {
		_, err = t.file.Seek(int64(currPageLen), stdio.SeekCurrent)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
		}
		page, err := t.insertEmptyPage()
		if err != nil {
			return nil, err
		}

		helper.Log.Debugf("Page full, inserting new one at offset %d", page.StartPos)

		lastPagePos = page.StartPos
		return page, err
	}

}

func (t *Table) insertEmptyPage() (*index.Page, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, datatype.TypePage); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	// length
	if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	if n != buf.Len() {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Expected %d, got %d", n, buf.Len()), platformerror.BinaryWriteErrorCode)
	}

	curPos, err := t.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	// startPos should point at the very first byte, that is types.TypePage and 5 bytes before the current pos
	startPos := curPos - (datatype.LenMeta)
	if startPos <= 0 {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unable to insert new page: start should be positive: %d", startPos),
			platformerror.PagePosViolationErrorCode)
	}
	return index.NewPage(startPos), nil
}

// insertIntoPage finds the first page that can fit buf and writes it into the page
func (t *Table) insertIntoPage(buf bytes.Buffer) (*index.Page, error) {
	page, err := t.seekToNextPage(uint32(buf.Len()))
	if err != nil {
		return nil, err
	}
	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	if n != buf.Len() {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Expected %d, got %d", n, buf.Len()), platformerror.BinaryWriteErrorCode)
	}
	// seek back to the beginning of the page
	if _, err = t.file.Seek(page.StartPos, stdio.SeekStart); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	return page, t.updatePageSize(page.StartPos, int32(buf.Len()))
}

// updatePageSize increases or decreases the size of a page by offset
// if the new size is 0, the page is removed
func (t *Table) updatePageSize(page int64, offset int32) (e error) {
	origPos, err := t.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	defer func() {
		_, err := t.file.Seek(origPos, stdio.SeekStart)
		e = err
	}()

	if _, err = t.file.Seek(page, stdio.SeekStart); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	dataType, err := t.reader.ReadByte()
	if err != nil {
		return err
	}
	if dataType != datatype.TypePage {
		return platformerror.NewStackTraceError(
			fmt.Sprintf("Table.updatePageSize: file pointer is at wrong position: expected: %d, actual: %d", datatype.TypePage, dataType),
			platformerror.InvalidDataTypeErrorCode)
	}
	length, err := t.reader.ReadUint32()
	if err != nil {
		return err
	}
	_, err = t.file.Seek(-1*datatype.LenInt32, stdio.SeekCurrent)
	if err != nil {
		return err
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
		return err
	}

	n, err := t.file.Write(b)
	if n != len(b) {
		return platformerror.NewStackTraceError(fmt.Sprintf("Expected %v, got %v", len(b), n), platformerror.BinaryWriteErrorCode)
	}

	if newLength == 0 {
		if err = t.removeEmptyPage(page); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) removeEmptyPage(page int64) (e error) {
	origPos, err := t.file.Seek(0, stdio.SeekCurrent)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	defer func() {
		_, err := t.file.Seek(origPos, stdio.SeekStart)
		e = err
	}()

	if _, err = t.file.Seek(page, stdio.SeekStart); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	dataType, err := t.reader.ReadByte()
	if err != nil {
		return err
	}
	if dataType != datatype.TypePage {
		return platformerror.NewStackTraceError(fmt.Sprintf("Expected %v, got %v", t, datatype.TypePage), platformerror.InvalidDataTypeErrorCode)
	}
	length, err := t.reader.ReadUint32()
	if err != nil {
		return err
	}
	if length != 0 {
		return platformerror.NewStackTraceError(fmt.Sprintf("New page not empty"), platformerror.InvalidPageErrorCode)
	}
	stat, err := t.file.Stat()
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
	}

	beforeReader := stdio.NewSectionReader(t.file, 0, page)
	afterReader := stdio.NewSectionReader(t.file, page+datatype.LenMeta, stat.Size())
	beforeBuf := make([]byte, page)

	if _, err = beforeReader.Read(beforeBuf); err != nil {
		return err
	}

	afterBuf := make([]byte, stat.Size()-(page+datatype.LenMeta))
	if _, err = afterReader.Read(afterBuf); err != nil {
		return err
	}

	if _, err = t.file.Seek(0, stdio.SeekStart); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	bw, err := t.file.Write(beforeBuf)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	aw, err := t.file.Write(afterBuf)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	if err = t.file.Truncate(int64(bw + aw)); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	return nil
}

// Close closes the table and the primaryKeyIndex files
func (t *Table) Close() error {
	if err := t.file.Close(); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.CloseErrorCode)
	}
	for _, idx := range t.indexes {
		if err := idx.Close(); err != nil {
			return platformerror.NewStackTraceError(err.Error(), platformerror.CloseErrorCode)
		}
	}
	return nil
}

func (t *Table) pageKey(pagePos int64) string {
	return fmt.Sprintf("%s-%d", t.Name, pagePos)
}

func (t *Table) invalidateCache(page *index.Page) {
	t.lru.Remove(t.pageKey(page.StartPos))
}
