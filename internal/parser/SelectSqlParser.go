package parser

import (
	"simple-database/internal/engine/table"
	configs "simple-database/internal/parser/grammar/select/configs"
	"simple-database/internal/platform/datatype"
	"simple-database/internal/platform/evaluator"
	"strconv"

	"github.com/antlr4-go/antlr/v4"
)

type SelectCommandASTVisitor struct {
}

func NewSelectCommandASTVisitor() *SelectCommandASTVisitor {
	return &SelectCommandASTVisitor{}
}

func (v *SelectCommandASTVisitor) Visit(tree antlr.ParseTree) interface{}         { return tree.Accept(v) }
func (v *SelectCommandASTVisitor) VisitChildren(_ antlr.RuleNode) interface{}     { return nil }
func (v *SelectCommandASTVisitor) VisitTerminal(_ antlr.TerminalNode) interface{} { return nil }
func (v *SelectCommandASTVisitor) VisitErrorNode(_ antlr.ErrorNode) interface{}   { return nil }

func (v *SelectCommandASTVisitor) VisitExpression(ctx *configs.ExpressionContext) interface{} {
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

func (v *SelectCommandASTVisitor) VisitPredicate(ctx *configs.PredicateContext) interface{} {
	left := v.Visit(ctx.Operand(0))
	right := v.Visit(ctx.Operand(1))
	op := ctx.Comparator().GetText()

	return &evaluator.Expression{Left: left, Op: datatype.FromSymbol(op), Right: right}
}

func (v *SelectCommandASTVisitor) VisitOperand(ctx *configs.OperandContext) interface{} {
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

func (v *SelectCommandASTVisitor) VisitLiteral(ctx *configs.LiteralContext) interface{} {
	text := ctx.GetText()

	// string
	if text[0] == '\'' {
		return text[1 : len(text)-1]
	}

	// number
	n, _ := strconv.ParseInt(text, 10, 64)
	return n
}

func (v *SelectCommandASTVisitor) VisitTypedLiteral(ctx *configs.TypedLiteralContext) interface{} {
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

func (v *SelectCommandASTVisitor) VisitColumn(ctx *configs.ColumnContext) interface{} {
	return ctx.GetText()
}

func (v *SelectCommandASTVisitor) VisitQuery(ctx *configs.QueryContext) interface{} {
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

func (v *SelectCommandASTVisitor) VisitSelectStatement(ctx *configs.SelectStatementContext) interface{} {
	command := table.SelectCommand{}
	selectColumns := v.Visit(ctx.SelectList())
	command.SelectColumns = selectColumns.([]string)

	tableName := v.Visit(ctx.TableName())
	command.TableName = tableName.(string)

	if ctx.WhereClause() != nil {
		expression := v.Visit(ctx.WhereClause())
		command.Expression = expression.(*evaluator.Expression)
	}

	if ctx.LimitClause() != nil {
		l := v.Visit(ctx.LimitClause())
		limit, _ := strconv.Atoi(l.(string))
		command.Limit = uint32(limit)
	}

	return command
}

func (v *SelectCommandASTVisitor) VisitWhereClause(ctx *configs.WhereClauseContext) interface{} {
	return v.Visit(ctx.Expression())
}

func (v *SelectCommandASTVisitor) VisitSelectList(ctx *configs.SelectListContext) interface{} {
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

func (v *SelectCommandASTVisitor) VisitComparator(ctx *configs.ComparatorContext) interface{} {
	return ctx.GetText()
}

func (v *SelectCommandASTVisitor) VisitTypeName(ctx *configs.TypeNameContext) interface{} {
	return ctx.GetText()
}

func (v *SelectCommandASTVisitor) VisitTableName(ctx *configs.TableNameContext) interface{} {
	return ctx.GetText()
}

func (v *SelectCommandASTVisitor) VisitLimitClause(ctx *configs.LimitClauseContext) interface{} {
	return ctx.INTEGER().GetText()
}
