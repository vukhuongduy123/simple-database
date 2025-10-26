package io

import (
	"fmt"
	"io"
	errors "simple-database/internal/platform/error"
)

type ColumnDefinitionWriter struct {
	w io.Writer
}

func (c *ColumnDefinitionWriter) Write(data []byte) (int, error) {
	n, err := c.w.Write(data)
	if err != nil {
		return n, fmt.Errorf("ColumneDefinitionWriter.Write: %w", err)
	}
	if n != len(data) {
		return n, fmt.Errorf(
			"ColumnDefinitionWriter.Write: %w", errors.NewIncompleteWriteError(n, len(data)),
		)
	}
	return n, nil
}

func NewColumnDefinitionWriter(w io.Writer) *ColumnDefinitionWriter {
	return &ColumnDefinitionWriter{
		w: w,
	}
}
