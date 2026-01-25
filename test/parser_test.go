package test

import (
	"simple-database/internal/parser"
	"simple-database/internal/platform/datatype"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSelect_WithWhere(t *testing.T) {
	sql := "SELECT a, b, c FROM age WHERE id > int32(1)"

	expression, err := parser.ParseSelect(sql)
	require.NoError(t, err)

	if expression.Op != datatype.OperatorGreater {
		t.Errorf("Expected id, got %s", expression.Op)
	}
	if expression.Right != int32(1) {
		t.Errorf("Expected 1, got %v", expression.Right)
	}
	if expression.Left != "id" {
		t.Errorf("Expected id, got %s", expression.Left)
	}
}

func TestParseSelect_WithoutWhere(t *testing.T) {
	sql := "SELECT a, b, c FROM age"

	expression, err := parser.ParseSelect(sql)
	require.NoError(t, err)
	if expression != nil {
		t.Errorf("Expected nil expression, got %v", expression)
	}
}
