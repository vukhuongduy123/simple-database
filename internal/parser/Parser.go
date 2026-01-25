package parser

import (
	configs "simple-database/internal/parser/configs"
	"simple-database/internal/platform/evaluator"

	"github.com/antlr4-go/antlr/v4"
)

func ParseSelect(sql string) (*evaluator.Expression, error) {
	// 1. Turn raw string into ANTLR input
	is := antlr.NewInputStream(sql)

	// 2. Lexing: characters → tokens
	lexer := configs.NewSelectSqlGrammarLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 3. Parsing: tokens → parse tree
	parser := configs.NewSelectSqlGrammarParser(stream)
	parser.BuildParseTrees = true

	// 4. Parse starting rule (entry point)
	tree := parser.Query()

	// 5. Walk parse a tree and build your AST
	visitor := &ASTVisitor{}
	result := tree.Accept(visitor)

	if result == nil {
		return nil, nil
	}

	// 6. Cast to your type and return
	return result.(*evaluator.Expression), nil
}
