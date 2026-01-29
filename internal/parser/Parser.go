package parser

import (
	"simple-database/internal/engine/table"
	deletegrammar "simple-database/internal/parser/grammar/delete/configs"
	insertgrammar "simple-database/internal/parser/grammar/insert/configs"
	selectgrammar "simple-database/internal/parser/grammar/select/configs"
	updategrammar "simple-database/internal/parser/grammar/update/configs"

	"github.com/antlr4-go/antlr/v4"
)

func ParseSelect(sql string) (table.SelectCommand, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := selectgrammar.NewSelectSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := selectgrammar.NewSelectSqlGrammarParser(stream)
	parser.BuildParseTrees = true

	// 4. Parse starting rule (entry point)
	tree := parser.Query()

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

	// 4. Parse starting rule (entry point)
	tree := parser.Query()

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

	// 4. Parse starting rule (entry point)
	tree := parser.Query()

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

	// 4. Parse starting rule (entry point)
	tree := parser.Query()

	// 5. Walk parse a tree and build your AST
	visitor := NewInsertCommandASTVisitor()
	result := tree.Accept(visitor)

	if result == nil {
		return table.InsertCommand{}, nil
	}

	// 6. Cast to your type and UpdateCommand
	return result.(table.InsertCommand), nil
}
