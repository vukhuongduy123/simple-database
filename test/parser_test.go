package test

import (
	"simple-database/internal/parser"
	"simple-database/internal/platform/datatype"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSelect_WithWhere(t *testing.T) {
	sql := "SELECT a, b, c FROM age WHERE id > int32(1) LIMIT 100"

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
	if selectCommand.Limit != 100 {
		t.Errorf("Expected 100, got %d", selectCommand.Limit)
	}
}

func TestParseSelect_Nested(t *testing.T) {
	sql := "SELECT a, b, c FROM age WHERE id > int32(1) AND age < int32(2) AND a >= int32(3) AND b <= int32(4)"

	selectCommand, err := parser.ParseSelect(sql)
	require.NoError(t, err)

	if selectCommand.TableName != "age" {
		t.Errorf("Expected age, got %s", selectCommand.TableName)
	}
	if len(selectCommand.SelectColumns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(selectCommand.SelectColumns))
	}
}

func TestParseSelect(t *testing.T) {
	sql := " SELECT * FROM users WHERE age <= INT32(129) LIMIT 10000000"

	selectCommand, err := parser.ParseSelect(sql)
	require.NoError(t, err)

	if selectCommand.TableName != "users" {
		t.Errorf("Expected age, got %s", selectCommand.TableName)
	}
	if selectCommand.Limit != 10000000 {
		t.Errorf("Expected 10000000, got %d", selectCommand.Limit)
	}
	if selectCommand.Expression.Left != "age" {
		t.Errorf("Expected age, got %s", selectCommand.Expression.Left)
	}
	if selectCommand.Expression.Right != int32(129) {
		t.Errorf("Expected 129, got %v", selectCommand.Expression.Right)
	}
}

func TestParseUpdate_WithWhere(t *testing.T) {
	sql := "UPDATE age SET a = int32(1), b = int32(2), c = int32(3) WHERE id > int32(1) LIMIT 100"

	updateCommand, err := parser.ParseUpdate(sql)
	require.NoError(t, err)

	if updateCommand.TableName != "age" {
		t.Errorf("Expected age, got %s", updateCommand.TableName)
	}
	if updateCommand.Expression.Op != datatype.OperatorGreater {
		t.Errorf("Expected id, got %s", updateCommand.Expression.Op)
	}
	if updateCommand.Expression.Right != int32(1) {
		t.Errorf("Expected 1, got %v", updateCommand.Expression.Right)
	}
	if updateCommand.Expression.Left != "id" {
		t.Errorf("Expected id, got %s", updateCommand.Expression.Left)
	}
	if len(updateCommand.Record) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(updateCommand.Record))
	}
}
