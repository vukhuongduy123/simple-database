package error

import "fmt"

type UnsupportedDataTypeError struct {
	DataType string
}

type DatabaseAlreadyExistsError struct {
	name string
}

type DatabaseDoesNotExistError struct {
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

type IncompleteReadError struct {
	expectedBytes int
	actualBytes   int
}

type UnknownColumnError struct {
	name string
}

type ColumnNotNullableError struct {
	name string
}

type MismatchingColumnsError struct {
	expected int
	actual   int
}

func (e *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("TLV: unsupported data datatype: %s", e.DataType)
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

func NewIncompleteReadError(expectedBytes int, actualBytes int) *IncompleteReadError {
	return &IncompleteReadError{expectedBytes: expectedBytes, actualBytes: actualBytes}
}

func NewNameTooLongError(maxLength, actualLength int) *NameTooLongError {
	return &NameTooLongError{maxLength: maxLength, actualLength: actualLength}
}

func NewUnknownColumnError(name string) *UnknownColumnError {
	return &UnknownColumnError{name: name}
}

func NewDatabaseDoesNotExistError(name string) *DatabaseDoesNotExistError {
	return &DatabaseDoesNotExistError{name: name}
}

func NewColumnNotNullableError(name string) *ColumnNotNullableError {
	return &ColumnNotNullableError{name: name}
}

func NewMismatchingColumnsError(expected, actual int) *MismatchingColumnsError {
	return &MismatchingColumnsError{expected: expected, actual: actual}
}

func (e *TableAlreadyExistsError) Error() string {
	return fmt.Sprintf("Table already exists: %s", e.name)
}

func (e *DatabaseAlreadyExistsError) Error() string {
	return fmt.Sprintf("Database already exists: %s", e.name)
}

func (e *DatabaseDoesNotExistError) Error() string {
	return fmt.Sprintf("Database doesnot exists: %s", e.name)
}

func (e *CannotCreateTableExistsError) Error() string {
	return e.name
}

func (e *IncompleteWriteError) Error() string {
	return fmt.Sprintf("incomplete write: expected to write %d bytes, but %d bytes were written", e.expectedBytes, e.actualBytes)
}

func (e *IncompleteReadError) Error() string {
	return fmt.Sprintf("incomplete read: expected to write %d bytes, but %d bytes were written", e.expectedBytes, e.actualBytes)
}

func (e *NameTooLongError) Error() string {
	return fmt.Sprintf("column name cannot be larger than %d characters. %d given", e.maxLength, e.actualLength)
}

func (e *UnknownColumnError) Error() string {
	return fmt.Sprintf("Column not exists %s", e.name)
}

func (e *ColumnNotNullableError) Error() string {
	return fmt.Sprintf("Column %s not nullable", e.name)
}

func (e *MismatchingColumnsError) Error() string {
	return fmt.Sprintf("column number mismatch: expected: %d, actual: %d", e.expected, e.actual)
}
