package io

import (
	"fmt"
	"io"
	platformerror "simple-database/internal/platform/error"
)

type ColumnDefinitionWriter struct {
	w io.Writer
}

func (c *ColumnDefinitionWriter) Write(data []byte) (int, error) {
	n, err := c.w.Write(data)
	if err != nil {
		return n, platformerror.NewStackTraceError(err.Error(), platformerror.IncompleteWriteErrorCode)
	}
	if n != len(data) {
		return n, platformerror.NewStackTraceError(fmt.Sprintf("Expected %d, get %d", len(data), n), platformerror.IncompleteWriteErrorCode)
	}
	return n, nil
}

func NewColumnDefinitionWriter(w io.Writer) *ColumnDefinitionWriter {
	return &ColumnDefinitionWriter{
		w: w,
	}
}
