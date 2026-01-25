package test

import (
	"simple-database/internal/parser"
	"simple-database/internal/platform/datatype"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSelect_WithWhere(t *testing.T) {
	sql := "SELECT a, b, c FROM age WHERE id > int32(1)"

	selectCommand, err := parser.ParseSelect(sql)
	require.NoError(t, err)

	if selectCommand.Expression.Op != datatype.OperatorGreater {
		t.Errorf("Expected id, got %s", selectCommand.Expression.Op)
	}
	if selectCommand.Expression.Right != int32(1) {
		t.Errorf("Expected 1, got %v", selectCommand.Expression.Right)
	}
	if selectCommand.Expression.Left != "id" {
		t.Errorf("Expected id, got %s", selectCommand.Expression.Left)
	}
	if selectCommand.TableName != "age" {
		t.Errorf("Expected age, got %s", selectCommand.TableName)
	}
	if len(selectCommand.SelectColumns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(selectCommand.SelectColumns))
	}
}

func TestParseSelect_Nested(t *testing.T) {
	sql := "SELECT a, b, c FROM age WHERE id > int32(1) AND age > int32(2)"

	selectCommand, err := parser.ParseSelect(sql)
	require.NoError(t, err)

	if selectCommand.TableName != "age" {
		t.Errorf("Expected age, got %s", selectCommand.TableName)
	}
	if len(selectCommand.SelectColumns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(selectCommand.SelectColumns))
	}
}
