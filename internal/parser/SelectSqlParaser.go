package parser

import (
	"simple-database/internal/engine/table"
	configs "simple-database/internal/parser/configs"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/evaluator"
	"simple-database/internal/platform/helper"
	"strconv"

	"github.com/antlr4-go/antlr/v4"
)

type ASTVisitor struct {
	selectCommand *table.SelectCommand
}

func NewASTVisitor() *ASTVisitor {
	return &ASTVisitor{selectCommand: &table.SelectCommand{}}
}

func (v *ASTVisitor) Visit(tree antlr.ParseTree) interface{}         { return tree.Accept(v) }
func (v *ASTVisitor) VisitChildren(_ antlr.RuleNode) interface{}     { return nil }
func (v *ASTVisitor) VisitTerminal(_ antlr.TerminalNode) interface{} { return nil }
func (v *ASTVisitor) VisitErrorNode(_ antlr.ErrorNode) interface{}   { return nil }

func (v *ASTVisitor) VisitExpression(ctx *configs.ExpressionContext) interface{} {
	if ctx.Predicate() != nil {
		return v.Visit(ctx.Predicate())
	}

	// expression AND expression | expression OR expression
	left := v.Visit(ctx.Expression(0))
	child := ctx.GetChild(1)

	pt, ok := child.(antlr.ParseTree)
	if !ok {
		panic("child is not a ParseTree")
	}

	op := pt.GetText()

	right := v.Visit(ctx.Expression(1))

	return &evaluator.Expression{Left: left, Op: datatype.FromSymbol(op), Right: right}
}

func (v *ASTVisitor) VisitPredicate(ctx *configs.PredicateContext) interface{} {
	left := v.Visit(ctx.Operand(0))
	right := v.Visit(ctx.Operand(1))
	op := ctx.Comparator().GetText()

	return &evaluator.Expression{Left: left, Op: datatype.FromSymbol(op), Right: right}
}

func (v *ASTVisitor) VisitOperand(ctx *configs.OperandContext) interface{} {
	if ctx.Column() != nil {
		return v.Visit(ctx.Column())
	}

	// Column
	if ctx.Column() != nil {
		return v.Visit(ctx.Column())
	}

	// Typed literal
	if ctx.TypedLiteral() != nil {
		return v.Visit(ctx.TypedLiteral())
	}

	return nil
}

func (v *ASTVisitor) VisitLiteral(ctx *configs.LiteralContext) interface{} {
	text := ctx.GetText()

	// string
	if text[0] == '\'' {
		return text[1 : len(text)-1]
	}

	// number
	n, _ := strconv.ParseInt(text, 10, 64)
	return n
}

func (v *ASTVisitor) VisitTypedLiteral(ctx *configs.TypedLiteralContext) interface{} {
	typeName := ctx.TypeName().GetText()
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

func (v *ASTVisitor) VisitColumn(ctx *configs.ColumnContext) interface{} {
	helper.Log.Debugf("Visiting column")
	return ctx.GetText()
}

func (v *ASTVisitor) VisitQuery(ctx *configs.QueryContext) interface{} {
	if ctx.GetChildCount() == 0 {
		return nil
	}

	// Instead of forcing a cast, use the generated helper method if available
	if stmt := ctx.SelectStatement(); stmt != nil {
		return v.Visit(stmt)
	}

	// Fallback if grammar is complex
	child, ok := ctx.GetChild(0).(antlr.ParseTree)
	if !ok {
		return nil
	}
	return v.Visit(child)
}

func (v *ASTVisitor) VisitSelectStatement(ctx *configs.SelectStatementContext) interface{} {
	selectColumns := v.Visit(ctx.SelectList())
	v.selectCommand.SelectColumns = selectColumns.([]string)

	tableName := v.Visit(ctx.TableName())
	v.selectCommand.TableName = tableName.(string)

	if ctx.WhereClause() != nil {
		expression := v.Visit(ctx.WhereClause())
		v.selectCommand.Expression = expression.(*evaluator.Expression)
	}
	return v.selectCommand
}

func (v *ASTVisitor) VisitWhereClause(ctx *configs.WhereClauseContext) interface{} {
	return v.Visit(ctx.Expression())
}

func (v *ASTVisitor) VisitSelectList(ctx *configs.SelectListContext) interface{} {
	if ctx.STAR() != nil {
		return []string{"*"}
	}

	// case: column (',' column)*
	cols := make([]string, 0, len(ctx.AllColumn()))
	for _, col := range ctx.AllColumn() {
		cols = append(cols, col.GetText())
	}

	return cols
}

func (v *ASTVisitor) VisitComparator(ctx *configs.ComparatorContext) interface{} {
	return ctx.GetText()
}

func (v *ASTVisitor) VisitTypeName(ctx *configs.TypeNameContext) interface{} {
	return ctx.GetText()
}

func (v *ASTVisitor) VisitTableName(ctx *configs.TableNameContext) interface{} {
	return ctx.GetText()
}
