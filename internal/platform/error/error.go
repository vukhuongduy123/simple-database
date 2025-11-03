package error

import (
	"errors"
	"fmt"
	"runtime"
)

// StackError wraps any error and captures a stack trace
type StackError struct {
	err   error
	stack string
}

// WrapError wraps any error and captures the current stack trace
func WrapError(err error) *StackError {
	if err == nil {
		return nil
	}
	buf := make([]byte, 1024*8)
	n := runtime.Stack(buf, false)
	return &StackError{
		err:   err,
		stack: string(buf[:n]),
	}
}

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

type InvalidFilename struct {
	filename string
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

func NewInvalidFilename(filename string) *InvalidFilename {
	return &InvalidFilename{filename: filename}
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
	return fmt.Sprintf("Cannot create table: %s. Error: %s", e.name, e.err.Error())
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

func (e *StackError) Error() string {
	return fmt.Sprintf("%s\nStack trace:\n%s", e.err.Error(), e.stack)
}

func (e *InvalidFilename) Error() string {
	return fmt.Sprintf("invalid filename: %s", e.filename)
}

type ItemNotFoundError struct {
	id int64
}

func NewItemNotFoundError(id int64) *ItemNotFoundError {
	return &ItemNotFoundError{id: id}
}

func (e *ItemNotFoundError) Error() string {
	return fmt.Sprintf("item with ID %d not found in index", e.id)
}

type ItemNotInLinkedListError struct {
	values any
	item   any
}

func NewItemNotInLinkedListError(values, item any) *ItemNotInLinkedListError {
	return &ItemNotInLinkedListError{values: values, item: item}
}

func (e *ItemNotInLinkedListError) Error() string {
	return fmt.Sprintf("item %v not found in %v", e.item, e.values)
}

func (e *ItemNotInLinkedListError) Is(target error) bool {
	var errItemNotFound *ItemNotInLinkedListError
	return errors.As(target, &errItemNotFound)
}
