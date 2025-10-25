package table

import (
	"fmt"
	"os"
	"simple-database/internal/table/column"
	"simple-database/internal/table/column/io"
)

type Columns map[string]*column.Column

const FileExtension = ".bin"

type Table struct {
	Name        string
	file        *os.File
	columnNames []string
	columns     Columns
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

func NewTableWithColumns(file *os.File, columns Columns, columnNames []string) (*Table, error) {
	return &Table{
		file:        file,
		columnNames: columnNames,
		columns:     columns,
	}, nil
}
