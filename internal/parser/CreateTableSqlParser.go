package parser

import (
	"simple-database/internal/engine"
	"simple-database/internal/engine/table"
	"simple-database/internal/engine/table/column"
	configs "simple-database/internal/parser/grammar/create/configs"
	"simple-database/internal/platform"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/helper"
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

type CreateTableCommandASTVisitor struct {
}

func NewCreateTableCommandASTVisitor() *CreateTableCommandASTVisitor {
	return &CreateTableCommandASTVisitor{}
}

func (v *CreateTableCommandASTVisitor) Visit(tree antlr.ParseTree) interface{}         { return tree.Accept(v) }
func (v *CreateTableCommandASTVisitor) VisitChildren(_ antlr.RuleNode) interface{}     { return nil }
func (v *CreateTableCommandASTVisitor) VisitTerminal(_ antlr.TerminalNode) interface{} { return nil }
func (v *CreateTableCommandASTVisitor) VisitErrorNode(_ antlr.ErrorNode) interface{}   { return nil }

func (v *CreateTableCommandASTVisitor) VisitQuery(ctx *configs.QueryContext) interface{} {
	if ctx.GetChildCount() == 0 {
		return nil
	}

	if stmt := ctx.CreateTableStatement(); stmt != nil {
		return v.Visit(stmt)
	}

	// Fallback if grammar is complex
	child, ok := ctx.GetChild(0).(antlr.ParseTree)
	if !ok {
		return nil
	}
	return v.Visit(child)
}

func (v *CreateTableCommandASTVisitor) VisitCreateTableStatement(ctx *configs.CreateTableStatementContext) interface{} {
	command := engine.CreateTableCommand{}

	tableName := v.Visit(ctx.TableName()).(string)
	command.TableName = tableName

	columns := table.Columns{}
	for _, colCtx := range ctx.AllColumnExpression() {
		col := v.Visit(colCtx).(*column.Column)
		columns[helper.ToString(col.Name[:])] = col
	}
	command.Columns = columns

	return command
}

func (v *CreateTableCommandASTVisitor) VisitTableName(ctx *configs.TableNameContext) interface{} {
	return ctx.GetText()
}

func (v *CreateTableCommandASTVisitor) VisitColumnExpression(ctx *configs.ColumnExpressionContext) interface{} {
	colName := v.Visit(ctx.Column()).(string)
	colType := v.Visit(ctx.ColumnDefinition()).(platform.Pair[byte, int32])

	col, err := column.NewColumn(colName, colType.First, colType.Second)
	if err != nil {
		panic(err)
	}
	return col
}

func (v *CreateTableCommandASTVisitor) VisitColumn(ctx *configs.ColumnContext) interface{} {
	return ctx.IDENTIFIER().GetText()
}

func (v *CreateTableCommandASTVisitor) VisitColumnDefinition(ctx *configs.ColumnDefinitionContext) interface{} {
	columnType := v.Visit(ctx.TypeName()).(byte)
	indexType := column.Normal
	if ctx.IndexType() != nil {
		indexType = v.Visit(ctx.IndexType()).(int)
	}

	return platform.Pair[byte, int32]{First: columnType, Second: int32(indexType)}
}

func (v *CreateTableCommandASTVisitor) VisitIndexType(ctx *configs.IndexTypeContext) interface{} {
	switch {
	case ctx.UNIQUE() != nil:
		return column.UsingUniqueIndex
	case ctx.INDEX() != nil:
		return column.UsingIndex
	case ctx.PRIMARY() != nil:
		return column.PrimaryKey
	default:
		return column.Normal
	}
}

func (v *CreateTableCommandASTVisitor) VisitTypeName(ctx *configs.TypeNameContext) interface{} {
	typeName := strings.ToLower(ctx.GetText())

	switch typeName {
	case "int32":
		return datatype.TypeInt32
	case "int64":
		return datatype.TypeInt64
	case "float64":
		return datatype.TypeFloat64
	case "float32":
		return datatype.TypeFloat32
	default:
		return datatype.TypeString
	}
}
