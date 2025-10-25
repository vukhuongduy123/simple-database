package internal

import (
	"fmt"
	"os"
	"path/filepath"
	errors "simple-database/internal/common/error"
	"simple-database/internal/table"
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

func (db *Database) CreateTable(name string, columnNames []string, columns table.Columns) (*table.Table, error) {
	path := path(name) + table.FileExtension
	if _, err := os.Open(path); err == nil {
		return nil, errors.NewTableAlreadyExistsError(name)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.NewCannotCreateTableError(err, name)
	}

	t, err := table.NewTableWithColumns(f, columns, columnNames)
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

func exists(name string) bool {
	_, err := os.ReadDir(path(name))
	return err == nil
}

func path(name string) string {
	return filepath.Join(BaseDir, name)
}
