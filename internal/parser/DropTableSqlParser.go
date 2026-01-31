package parser

import (
	"simple-database/internal/engine"
	configs "simple-database/internal/parser/grammar/drop/configs"

	"github.com/antlr4-go/antlr/v4"
)

type DropTableCommandASTVisitor struct {
}

func NewDropTableCommandASTVisitor() *DropTableCommandASTVisitor {
	return &DropTableCommandASTVisitor{}
}

func (v *DropTableCommandASTVisitor) Visit(tree antlr.ParseTree) interface{}         { return tree.Accept(v) }
func (v *DropTableCommandASTVisitor) VisitChildren(_ antlr.RuleNode) interface{}     { return nil }
func (v *DropTableCommandASTVisitor) VisitTerminal(_ antlr.TerminalNode) interface{} { return nil }
func (v *DropTableCommandASTVisitor) VisitErrorNode(_ antlr.ErrorNode) interface{}   { return nil }

func (v *DropTableCommandASTVisitor) VisitQuery(ctx *configs.QueryContext) interface{} {
	if ctx.GetChildCount() == 0 {
		return nil
	}

	if stmt := ctx.DropTableStatement(); stmt != nil {
		return v.Visit(stmt)
	}

	// Fallback if grammar is complex
	child, ok := ctx.GetChild(0).(antlr.ParseTree)
	if !ok {
		return nil
	}
	return v.Visit(child)
}

func (v *DropTableCommandASTVisitor) VisitDropTableStatement(ctx *configs.DropTableStatementContext) interface{} {
	command := engine.DropTableCommand{}
	command.TableName = v.Visit(ctx.TableName()).(string)

	return command
}

func (v *DropTableCommandASTVisitor) VisitTableName(ctx *configs.TableNameContext) interface{} {
	return ctx.GetText()
}
