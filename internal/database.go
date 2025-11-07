package internal

import (
	"fmt"
	"os"
	"path/filepath"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/helper"
	"simple-database/internal/table"
	"simple-database/internal/table/column"
	"strings"
)

const (
	BaseDir = "./data"
)

type Tables map[string]*table.Table

type Database struct {
	name   string
	path   string
	Tables Tables
}

func CreateDatabase(name string) (*Database, error) {
	if exists(name) {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Database %s already existed", name),
			platformerror.DatabaseAlreadyExistsErrorCode)
	}
	if err := os.MkdirAll(path(name), 0644); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
	}
	return &Database{
		name:   name,
		path:   path(name),
		Tables: make(Tables),
	}, nil
}

func NewDatabase(name string) (*Database, error) {
	if !exists(name) {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Database %s not existed", name),
			platformerror.DatabaseNotExistsErrorCode)
	}
	db := &Database{name: name, path: path(name)}
	tables, err := db.readTables()
	if err != nil {
		return nil, err
	}
	db.Tables = tables
	for _, t := range db.Tables {
		if err := t.RestoreWAL(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *Database) CreateTable(name string, columns table.Columns) (*table.Table, error) {
	dbPath := filepath.Join(path(db.name), name) + table.FileExtension
	if _, err := os.Open(dbPath); err == nil {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Table %s already existed", name),
			platformerror.TableAlreadyExistsErrorCode)
	}
	f, err := os.Create(dbPath)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
	}

	if err := validateColumnsConstraint(columns); err != nil {
		return nil, err
	}

	t, err := table.NewTableWithColumns(f, columns)

	if err != nil {
		return nil, err
	}

	db.Tables[name] = t
	return t, nil
}

func (db *Database) readTables() (Tables, error) {
	tablePaths, err := os.ReadDir(path(db.name))
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
	}

	tables := make([]*table.Table, 0)

	for _, v := range tablePaths {
		if strings.Contains(v.Name(), "_wal") || strings.Contains(v.Name(), "_idx") {
			continue
		}

		if _, err := v.Info(); err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
		}
		f, err := os.OpenFile(filepath.Join(db.path, v.Name()), os.O_RDWR, 0777)
		if err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
		}

		t, err := table.NewTable(f)
		if err != nil {
			return nil, err
		}

		tables = append(tables, t)
	}

	tablesMap := make(Tables)
	for _, v := range tables {
		tablesMap[v.Name] = v
	}
	return tablesMap, nil
}

func exists(name string) bool {
	_, err := os.ReadDir(path(name))
	return err == nil
}

func path(name string) string {
	return filepath.Join(BaseDir, name)
}

func validateColumnsConstraint(columns table.Columns) error {
	existedName := make(map[string]any)

	numberOfPrimaryKeys := 0

	for _, c := range columns {
		if _, existed := existedName[helper.ToString(c.Name[:])]; existed {
			return platformerror.NewStackTraceError(fmt.Sprintf("Column %s already existed", helper.ToString(c.Name[:])),
				platformerror.ColumnAlreadyExistsErrorCode)
		}

		if c.Is(column.PrimaryKey) == true {
			numberOfPrimaryKeys++
			if numberOfPrimaryKeys > 1 {
				return platformerror.NewStackTraceError("There are more than one primary key exist",
					platformerror.InvalidNumberOfPrimaryKeysErrorCode)
			}
		}
	}

	if numberOfPrimaryKeys == 0 {
		return platformerror.NewStackTraceError("There must be one primary key exist",
			platformerror.InvalidNumberOfPrimaryKeysErrorCode)
	}

	return nil
}

func (db *Database) Close() error {
	var e error
	for _, t := range db.Tables {
		if err := t.Close(); err != nil {
			e = err
		}
	}
	return e
}
