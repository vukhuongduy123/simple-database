package internal

import (
	"fmt"
	"os"
	"path/filepath"
	errors "simple-database/internal/platform/error"
	"simple-database/internal/platform/helper"
	io2 "simple-database/internal/platform/io"
	"simple-database/internal/table"
	"simple-database/internal/table/column/io"
	"simple-database/internal/table/column/parser"
	"simple-database/internal/table/wal"
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
		return nil, errors.NewDatabaseAlreadyExistsError(name)
	}
	if err := os.MkdirAll(path(name), 0644); err != nil {
		return nil, fmt.Errorf("CreateDatabase: %w", err)
	}
	return &Database{
		name:   name,
		path:   path(name),
		Tables: make(Tables),
	}, nil
}

func NewDatabase(name string) (*Database, error) {
	if !exists(name) {
		return nil, errors.NewDatabaseDoesNotExistError(name)
	}
	db := &Database{name: name, path: path(name)}
	tables, err := db.readTables()
	if err != nil {
		return nil, fmt.Errorf("NewDatabase: %w", err)
	}
	db.Tables = tables
	for _, t := range db.Tables {
		if err := t.RestoreWAL(); err != nil {
			return nil, fmt.Errorf("NewDatabase: %w", err)
		}
	}
	return db, nil
}

func (db *Database) CreateTable(name string, columnNames []string, columns table.Columns) (*table.Table, error) {
	path := filepath.Join(path(db.name), name) + table.FileExtension
	if _, err := os.Open(path); err == nil {
		return nil, errors.NewTableAlreadyExistsError(name)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.NewCannotCreateTableError(err, name)
	}

	if err := validateColumnsConstraint(columns); err != nil {
		return nil, errors.WrapError(errors.NewCannotCreateTableError(err, name))
	}

	r := io2.NewReader(f)
	walFile, err := wal.NewWal(db.path, name)
	if err != nil {
		return nil, errors.NewCannotCreateTableError(err, name)
	}

	t, err := table.NewTableWithColumns(f, columns, columnNames, r, io.NewColumnDefinitionReader(r), parser.NewRecordParser(f, columnNames), walFile)
	if err != nil {
		return nil, errors.NewCannotCreateTableError(err, name)
	}

	err = t.WriteColumnDefinitions()
	if err != nil {
		return nil, errors.NewCannotCreateTableError(err, name)
	}
	db.Tables[name] = t
	return t, nil
}

func (db *Database) readTables() (Tables, error) {
	tablePaths, err := os.ReadDir(path(db.name))
	if err != nil {
		return nil, fmt.Errorf("readTables: %w", err)
	}

	tables := make([]*table.Table, 0)

	for _, v := range tablePaths {
		if strings.Contains(v.Name(), "_wal") {
			continue
		}

		if _, err := v.Info(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		f, err := os.OpenFile(
			filepath.Join(db.path, v.Name()), os.O_APPEND|os.O_RDWR, 0777,
		)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		r := io2.NewReader(f)
		columnDefReader := io.NewColumnDefinitionReader(r)
		tableName, err := table.GetTableName(f)
		if err != nil {
			return nil, errors.NewCannotCreateTableError(err, v.Name())
		}

		walFile, err := wal.NewWal(db.path, tableName)
		if err != nil {
			return nil, errors.NewCannotCreateTableError(err, v.Name())
		}

		t, err := table.NewTable(f, r, columnDefReader, nil, walFile)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		if err := t.ReadColumnDefinitions(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		if err = t.SetRecordParser(parser.NewRecordParser(f, t.ColumnNames)); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
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
	hasUniquePrimaryKey := false

	for _, c := range columns {
		if _, existed := existedName[helper.ToString(c.Name[:])]; existed {
			return fmt.Errorf("duplicate column name: %s", helper.ToString(c.Name[:]))
		}
		existedName[helper.ToString(c.Name[:])] = ""
		if c.IsPrimaryKey && !hasUniquePrimaryKey {
			hasUniquePrimaryKey = true
		} else if c.IsPrimaryKey {
			return fmt.Errorf("duplicate primary key")
		}
	}

	return nil
}
