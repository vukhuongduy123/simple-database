package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"simple-database/internal/platform/datatype"
	platformerror "simple-database/internal/platform/error"
	platformio "simple-database/internal/platform/io"
	platformparser "simple-database/internal/platform/parser"
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

type RestorableData struct {
	LastEntry *Entry
	Data      []byte
}

func newRestorableData(lastEntry *Entry, data []byte) *RestorableData {
	return &RestorableData{
		LastEntry: lastEntry,
		Data:      data,
	}
}

func NewWal(dbPath string, tableName string) (*WAL, error) {
	path := filepath.Join(dbPath, fmt.Sprintf(FileNamePostfix, tableName))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
	}

	path = filepath.Join(dbPath, fmt.Sprintf(LastCommitedFileNamePostfix, tableName))
	lastCommitedFilePointer, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.OpenFileErrorCode)
	}

	return &WAL{
		file:             f,
		lastCommitedFile: lastCommitedFilePointer,
	}, nil
}

func (w *WAL) Append(op, table string, data []byte) (*Entry, error) {
	id := generateID()

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	walMarshaler := parser.NewWALMarshaler(id, op, table, data)
	buf, err := walMarshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if err := w.write(buf); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	return newEntry(id, uint32(len(data))), nil
}

func (w *WAL) Commit(entry *Entry) error {
	marshaler := parser.NewWALLastCommitedMarshaler(entry.ID, entry.Len)
	data, err := marshaler.MarshalBinary()
	if err != nil {
		return err
	}
	if err := os.WriteFile(w.lastCommitedFile.Name(), data, 0644); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
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
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	if n != len(buf) {
		return platformerror.NewStackTraceError(fmt.Sprintf("WAL.write: incomplete write. expected: %d, actual: %d", n, len(buf)),
			platformerror.ReadWalErrorCode)

	}
	return nil
}

func (w *WAL) readLastEntry(length uint32) (*Entry, error) {
	if _, err := w.file.Seek(-1*int64(length), io.SeekEnd); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	buf := make([]byte, length)
	n, err := w.file.Read(buf)
	if err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}

	if uint32(n) != length {
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("WAL.write: incomplete write. expected: %d, actual: %d", n, len(buf)),
			platformerror.ReadWalErrorCode)
	}

	byteUnmarshaler := platformparser.NewValueUnmarshaler[byte]()
	intUnmarshaler := platformparser.NewValueUnmarshaler[uint32]()
	bytesRead := 0

	// type
	if err = byteUnmarshaler.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	bytesRead += datatype.LenByte

	// length
	if err = intUnmarshaler.UnmarshalBinary(buf[bytesRead:]); err != nil {
		return nil, err
	}
	bytesRead += datatype.LenInt32

	strUnmarshaler := platformparser.NewValueUnmarshaler[string]()
	tlvUnmarshaler := platformparser.NewTLVUnmarshaler(strUnmarshaler)

	// ID
	if err = tlvUnmarshaler.UnmarshalBinary(buf[bytesRead:]); err != nil {
		return nil, err
	}
	bytesRead += len(tlvUnmarshaler.Value)
	id := tlvUnmarshaler.Value

	return &Entry{ID: id, Len: length}, nil
}

func (w *WAL) GetRestorableData() (*RestorableData, error) {
	if _, err := w.lastCommitedFile.Seek(0, io.SeekStart); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	data := make([]byte, 1024)
	n, err := w.lastCommitedFile.Read(data)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
	}

	data = data[:n]
	unmarshaler := parser.NewWALLastCommitedUnmarshaler()
	if err = unmarshaler.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	lastCommittedID := unmarshaler.ID

	lastEntry, err := w.readLastEntry(unmarshaler.Len)
	if err != nil {
		return nil, err
	}

	if lastEntry.ID == lastCommittedID {
		return nil, nil
	}

	buf, err := w.getRestorableData(lastCommittedID)
	if err != nil {
		return nil, err
	}

	return newRestorableData(lastEntry, buf), nil
}

func (w *WAL) skipEntry(id string, length uint32) error {
	_, err := w.file.Seek(int64(-1*len(id)), io.SeekCurrent)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	_, err = w.file.Seek(int64(length), io.SeekCurrent)
	if err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}
	return nil
}

func (w *WAL) getRestorableData(commitID string) ([]byte, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCodeCode)
	}

	r := platformio.NewReader(w.file)

	commitIDFound := false
	buf := bytes.Buffer{}
	for {
		t, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return buf.Bytes(), nil
			}
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryReadErrorCode)
		}
		if t != datatype.TypeWALEntry {
			return nil, platformerror.NewStackTraceError(fmt.Sprintf("Expected %v, got %v", datatype.TypeWALEntry, t),
				platformerror.InvalidDataTypeErrorCode)
		}

		length, err := r.ReadUint32()
		if err != nil {
			return nil, err
		}

		tlvParser := platformparser.NewTLVParser(r)
		val, err := tlvParser.Parse()
		id := val.(string)

		if err != nil {
			return nil, err
		}

		if id == commitID {
			commitIDFound = true
			if err = w.skipEntry(id, length); err != nil {
				return nil, err
			}
			continue
		}

		// We are before the commit ID so entry can be skipped entirely
		if !commitIDFound {
			if err = w.skipEntry(id, length); err != nil {
				return nil, err
			}
			continue
		}

		// We are after the commit, so this entry needs to be restored

		// op
		val, err = tlvParser.Parse()
		op := val.(string)
		if op != parser.OpInsert {
			return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unsupported operator: %v", op), platformerror.UnknownOperatorErrorCode)
		}

		// table
		val, err = tlvParser.Parse()

		// data
		t, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		if t != datatype.TypeRecord {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.InvalidDataTypeErrorCode)
		}

		length, err = r.ReadUint32()
		if err != nil {
			return nil, err
		}

		buf.WriteByte(t)
		if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
			return nil, platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
		}

		record := make([]byte, length)
		if _, err = r.Read(record); err != nil {
			return nil, err
		}
		buf.Write(record)
	}
}
