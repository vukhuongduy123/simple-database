package error

import "fmt"

type UnsupportedDataTypeError struct {
	DataType string
}

type DatabaseAlreadyExistsError struct {
	name string
}

type TableAlreadyExistsError struct {
	name string
}

type CannotCreateTableExistsError struct {
	name string
	err  error
}

type IncompleteWriteError struct {
	expectedBytes int
	actualBytes   int
}

type NameTooLongError struct {
	actualLength int
	maxLength    int
}

func (e *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("TLV: unsupported data type: %s", e.DataType)
}

func NewDatabaseAlreadyExistsError(name string) *DatabaseAlreadyExistsError {
	return &DatabaseAlreadyExistsError{name: name}
}

func NewTableAlreadyExistsError(name string) *TableAlreadyExistsError {
	return &TableAlreadyExistsError{name: name}
}

func NewCannotCreateTableError(err error, name string) *CannotCreateTableExistsError {
	return &CannotCreateTableExistsError{name: name, err: err}
}

func NewIncompleteWriteError(expectedBytes int, actualBytes int) *IncompleteWriteError {
	return &IncompleteWriteError{expectedBytes: expectedBytes, actualBytes: actualBytes}
}

func NewNameTooLongError(maxLength, actualLength int) *NameTooLongError {
	return &NameTooLongError{maxLength: maxLength, actualLength: actualLength}
}

func (e *TableAlreadyExistsError) Error() string {
	return fmt.Sprintf("Table already exists: %s", e.name)
}

func (e *DatabaseAlreadyExistsError) Error() string {
	return fmt.Sprintf("Database already exists: %s", e.name)
}

func (e *CannotCreateTableExistsError) Error() string {
	return e.name
}

func (e *IncompleteWriteError) Error() string {
	return fmt.Sprintf("incomplete write: expected to write %d bytes, but %d bytes were written", e.expectedBytes, e.actualBytes)
}

func (e *NameTooLongError) Error() string {
	return fmt.Sprintf("column name cannot be larger than %d characters. %d given", e.maxLength, e.actualLength)
}
