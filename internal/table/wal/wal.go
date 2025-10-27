package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"simple-database/internal/table/wal/parser"
	"strings"

	"github.com/google/uuid"
)

const (
	FileNamePostfix             = "%s_wal.bin"
	LastCommitedFileNamePostfix = "%s_wal_last_commited.bin"
)

type WAL struct {
	file             *os.File
	lastCommitedFile *os.File
}
type Entry struct {
	ID  string
	Len uint32
}

func NewWal(dbPath string, tableName string) (*WAL, error) {
	path := filepath.Join(dbPath, fmt.Sprintf(FileNamePostfix, tableName))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, fmt.Errorf("NewWal: %w", err)
	}

	path = filepath.Join(dbPath, fmt.Sprintf(LastCommitedFileNamePostfix, tableName))
	lastCommitedFilePointer, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, fmt.Errorf("NewWal: %w", err)
	}

	return &WAL{
		file:             f,
		lastCommitedFile: lastCommitedFilePointer,
	}, nil
}

func (w *WAL) Append(op, table string, data []byte) (*Entry, error) {
	id := generateID()

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}

	walMarshaler := parser.NewWALMarshaler(id, op, table, data)
	buf, err := walMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}
	if err := w.write(buf); err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}

	return newEntry(id, uint32(len(data))), nil
}

func (w *WAL) Commit(entry *Entry) error {
	marshaler := parser.NewWALLastCommitedMarshaler(entry.ID, entry.Len)
	data, err := marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("WAL.Commit: %w", err)
	}
	if err := os.WriteFile(w.lastCommitedFile.Name(), data, 0644); err != nil {
		return fmt.Errorf("WAL.Commit: %w", err)
	}
	return nil
}

func newEntry(id string, len uint32) *Entry {
	return &Entry{
		ID:  id,
		Len: len,
	}
}

func generateID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func (w *WAL) write(buf []byte) error {
	n, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL.write: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf(
			"WAL.write: incomplete write. expected: %d, actual: %d",
			n,
			len(buf),
		)
	}
	return nil
}
