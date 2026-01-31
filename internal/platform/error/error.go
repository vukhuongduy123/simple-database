package error

import (
	"fmt"
	"runtime"
)

type Code uint32

const (
	IncompleteReadErrorCode Code = iota
	IncompleteWriteErrorCode
	UnknownDatatypeErrorCode
	UnknownOperatorErrorCode
	UniqueKeyViolationErrorCode
	BinaryWriteErrorCode
	BinaryReadErrorCode
	InvalidNameLengthErrorCode
	OpenFileErrorCode
	FileSeekErrorCode
	InvalidDataTypeErrorCode
	InvalidTableName
	MissingColumnErrorCode
	ColumnViolationErrorCode
	PagePosViolationErrorCode
	InvalidPageErrorCode
	DatabaseAlreadyExistsErrorCode
	DatabaseNotExistsErrorCode
	TableAlreadyExistsErrorCode
	ColumnAlreadyExistsErrorCode
	CloseErrorCode
	InvalidNumberOfPrimaryKeysErrorCode
	BTreeReadError
	BTreeWriteError
	DeleteFileErrorCode
)

// StackTraceError wraps any error and captures a stack trace
type StackTraceError struct {
	Msg       string
	Stack     string
	ErrorCode Code
}

func NewStackTraceError(msg string, errorCode Code) *StackTraceError {
	buf := make([]byte, 1024*8)
	n := runtime.Stack(buf, false)
	return &StackTraceError{Msg: msg, Stack: string(buf[:n]), ErrorCode: errorCode}
}

func (e *StackTraceError) Error() string {
	return fmt.Sprintf("%s\nStack trace:\n%s", e.Msg, e.Stack)
}
