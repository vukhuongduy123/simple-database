package parser

import (
	"simple-database/internal/engine/table"
	configs "simple-database/internal/parser/grammar/insert/configs"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

type InsertCommandASTVisitor struct {
}

func NewInsertCommandASTVisitor() *InsertCommandASTVisitor {
	return &InsertCommandASTVisitor{}
}

func (v *InsertCommandASTVisitor) Visit(tree antlr.ParseTree) interface{}         { return tree.Accept(v) }
func (v *InsertCommandASTVisitor) VisitChildren(_ antlr.RuleNode) interface{}     { return nil }
func (v *InsertCommandASTVisitor) VisitTerminal(_ antlr.TerminalNode) interface{} { return nil }
func (v *InsertCommandASTVisitor) VisitErrorNode(_ antlr.ErrorNode) interface{}   { return nil }

func (v *InsertCommandASTVisitor) VisitQuery(ctx *configs.QueryContext) interface{} {
	if ctx.GetChildCount() == 0 {
		return nil
	}

	// Instead of forcing a cast, use the generated helper method if available
	if stmt := ctx.InsertStatement(); stmt != nil {
		return v.Visit(stmt)
	}

	// Fallback if grammar is complex
	child, ok := ctx.GetChild(0).(antlr.ParseTree)
	if !ok {
		return nil
	}
	return v.Visit(child)
}

func (v *InsertCommandASTVisitor) VisitInsertStatement(ctx *configs.InsertStatementContext) interface{} {
	insertCommand := table.InsertCommand{}

	insertCommand.TableName = v.Visit(ctx.TableName()).(string)

	columns := v.Visit(ctx.InsertColumns()).([]string)
	values := v.Visit(ctx.InsertValues()).([]any)

	record := make(map[string]any)
	for i, column := range columns {
		record[column] = values[i]
	}
	insertCommand.Record = record

	return insertCommand
}

func (v *InsertCommandASTVisitor) VisitInsertColumns(ctx *configs.InsertColumnsContext) interface{} {
	columns := make([]string, 0)
	for _, child := range ctx.AllColumn() {
		columns = append(columns, v.Visit(child).(string))
	}
	return columns
}

func (v *InsertCommandASTVisitor) VisitInsertValues(ctx *configs.InsertValuesContext) interface{} {
	values := make([]any, 0)
	for _, child := range ctx.AllTypedLiteral() {
		values = append(values, v.Visit(child))
	}
	return values
}

func (v *InsertCommandASTVisitor) VisitTypedLiteral(ctx *configs.TypedLiteralContext) interface{} {
	typeName := strings.ToLower(ctx.TypeName().GetText())
	value := ctx.Literal().GetText()

	switch typeName {
	case "int32":
		n, _ := strconv.ParseInt(value, 10, 32)
		return int32(n)
	case "int64":
		n, _ := strconv.ParseInt(value, 10, 64)
		return n
	case "float64":
		n, _ := strconv.ParseFloat(value, 64)
		return n
	case "float32":
		n, _ := strconv.ParseFloat(value, 32)
		return n
	default:
		return value
	}
}

func (v *InsertCommandASTVisitor) VisitColumn(ctx *configs.ColumnContext) interface{} {
	return ctx.GetText()
}

func (v *InsertCommandASTVisitor) VisitTypeName(ctx *configs.TypeNameContext) interface{} {
	return ctx.GetText()
}

func (v *InsertCommandASTVisitor) VisitTableName(ctx *configs.TableNameContext) interface{} {
	return ctx.GetText()
}

func (v *InsertCommandASTVisitor) VisitLiteral(ctx *configs.LiteralContext) interface{} {
	text := ctx.GetText()

	// string
	if text[0] == '\'' {
		return text[1 : len(text)-1]
	}

	// number
	n, _ := strconv.ParseInt(text, 10, 64)
	return n
}
