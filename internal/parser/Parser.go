package parser

import (
	"fmt"
	"simple-database/internal/engine"
	"simple-database/internal/engine/table"
	creategrammar "simple-database/internal/parser/grammar/create/configs"
	deletegrammar "simple-database/internal/parser/grammar/delete/configs"
	dropgrammar "simple-database/internal/parser/grammar/drop/configs"
	insertgrammar "simple-database/internal/parser/grammar/insert/configs"
	selectgrammar "simple-database/internal/parser/grammar/select/configs"
	updategrammar "simple-database/internal/parser/grammar/update/configs"

	platformerror "simple-database/internal/platform/error"

	"github.com/antlr4-go/antlr/v4"
)

type ErrorListener struct {
	*antlr.DefaultErrorListener
	Errors []string
}

func NewErrorListener() *ErrorListener {
	return &ErrorListener{
		DefaultErrorListener: antlr.NewDefaultErrorListener(),
		Errors:               []string{},
	}
}

func (l *ErrorListener) SyntaxError(
	_ antlr.Recognizer,
	_ interface{},
	line, column int,
	msg string,
	_ antlr.RecognitionException,
) {
	l.Errors = append(l.Errors,
		fmt.Sprintf("line %d:%d %s", line, column, msg))
}

func ParseSelect(sql string) (table.SelectCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := selectgrammar.NewSelectSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := selectgrammar.NewSelectSqlGrammarParser(stream)
	parser.BuildParseTrees = true
	listener := NewErrorListener()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)

	// 4. Parse starting rule (entry point)
	tree := parser.Query()

	if len(listener.Errors) > 0 {
		return table.SelectCommand{},
			platformerror.NewStackTraceError(fmt.Sprintf("Syntax error: %v", listener.Errors), platformerror.ParsingGrammarErrorCode)
	}

	// 5. Walk parse a tree and build your AST
	visitor := NewSelectCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return table.SelectCommand{}, nil
	}

	// 6. Cast to your type and return
	return result.(table.SelectCommand), nil
}

func ParseUpdate(sql string) (table.UpdateCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := updategrammar.NewUpdateSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := updategrammar.NewUpdateSqlGrammarParser(stream)
	parser.BuildParseTrees = true
	listener := NewErrorListener()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)

	// 4. Parse starting rule (entry point)
	tree := parser.Query()
	if len(listener.Errors) > 0 {
		return table.UpdateCommand{},
			platformerror.NewStackTraceError(fmt.Sprintf("Syntax error: %v", listener.Errors), platformerror.ParsingGrammarErrorCode)
	}

	// 5. Walk parse a tree and build your AST
	visitor := NewUpdateCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return table.UpdateCommand{}, nil
	}

	// 6. Cast to your type and UpdateCommand
	return result.(table.UpdateCommand), nil
}

func ParseDelete(sql string) (table.DeleteCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := deletegrammar.NewDeleteSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := deletegrammar.NewDeleteSqlGrammarParser(stream)
	parser.BuildParseTrees = true
	listener := NewErrorListener()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)

	// 4. Parse starting rule (entry point)
	tree := parser.Query()
	if len(listener.Errors) > 0 {
		return table.DeleteCommand{},
			platformerror.NewStackTraceError(fmt.Sprintf("Syntax error: %v", listener.Errors), platformerror.ParsingGrammarErrorCode)
	}

	// 5. Walk parse a tree and build your AST
	visitor := NewDeleteCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return table.DeleteCommand{}, nil
	}

	// 6. Cast to your type and UpdateCommand
	return result.(table.DeleteCommand), nil
}

func ParseInsert(sql string) (table.InsertCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := insertgrammar.NewInsertSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := insertgrammar.NewInsertSqlGrammarParser(stream)
	parser.BuildParseTrees = true
	listener := NewErrorListener()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)

	// 4. Parse starting rule (entry point)
	tree := parser.Query()
	if len(listener.Errors) > 0 {
		return table.InsertCommand{},
			platformerror.NewStackTraceError(fmt.Sprintf("Syntax error: %v", listener.Errors), platformerror.ParsingGrammarErrorCode)
	}

	// 5. Walk parse a tree and build your AST
	visitor := NewInsertCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return table.InsertCommand{}, nil
	}

	// 6. Cast to your type and UpdateCommand
	return result.(table.InsertCommand), nil
}

func ParseCreateTable(sql string) (engine.CreateTableCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := creategrammar.NewCreateTableSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := creategrammar.NewCreateTableSqlGrammarParser(stream)
	parser.BuildParseTrees = true
	listener := NewErrorListener()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)

	// 4. Parse starting rule (entry point)
	tree := parser.Query()
	if len(listener.Errors) > 0 {
		return engine.CreateTableCommand{},
			platformerror.NewStackTraceError(fmt.Sprintf("Syntax error: %v", listener.Errors), platformerror.ParsingGrammarErrorCode)
	}

	// 5. Walk parse a tree and build your AST
	visitor := NewCreateTableCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return engine.CreateTableCommand{}, nil
	}

	// 6. Cast to your type and UpdateCommand
	return result.(engine.CreateTableCommand), nil
}

func ParseDropTable(sql string) (engine.DropTableCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := dropgrammar.NewDropTableSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := dropgrammar.NewDropTableSqlGrammarParser(stream)
	parser.BuildParseTrees = true
	listener := NewErrorListener()
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)

	// 4. Parse starting rule (entry point)
	tree := parser.Query()
	if len(listener.Errors) > 0 {
		return engine.DropTableCommand{},
			platformerror.NewStackTraceError(fmt.Sprintf("Syntax error: %v", listener.Errors), platformerror.ParsingGrammarErrorCode)
	}

	// 5. Walk parse a tree and build your AST
	visitor := NewDropTableCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return engine.DropTableCommand{}, nil
	}

	// 6. Cast to your type and UpdateCommand
	return result.(engine.DropTableCommand), nil
}
