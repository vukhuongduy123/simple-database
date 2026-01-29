grammar InsertSqlGrammar;

/* =========
   Parser rules
   ========= */

query
    : insertStatement EOF
    ;

insertStatement
    : INSERT INTO tableName insertColumns VALUES insertValues
    ;

insertColumns
    : LPAREN column (COMMA column)* RPAREN
    ;

insertValues
    : LPAREN typedLiteral (COMMA typedLiteral)* RPAREN
    ;

typedLiteral
    : typeName LPAREN literal RPAREN
    ;

typeName
    : INT32 | INT64 | FLOAT32 | FLOAT64 | STRING_T
    ;

column
    : IDENTIFIER
    ;

tableName
    : IDENTIFIER
    ;

literal
    : NUMBER
    | INTEGER
    | STRING
    ;

/* =========
   Lexer rules
   ========= */
INSERT  : [Ii][Nn][Ss][Ee][Rr][Tt];
INTO    : [Ii][Nn][Tt][Oo];
VALUES  : [Vv][Aa][Ll][Uu][Ee][Ss];

INT32    : [Ii][Nn][Tt] '32';
INT64    : [Ii][Nn][Tt] '64';
FLOAT32  : [Ff][Ll][Oo][Aa][Tt] '32';
FLOAT64  : [Ff][Ll][Oo][Aa][Tt] '64';
STRING_T : [Ss][Tt][Rr][Ii][Nn][Gg];

STAR    : '*';
COMMA   : ',';
LPAREN  : '(';
RPAREN  : ')';

IDENTIFIER
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

INTEGER : [0-9]+;

NUMBER  : [0-9]+ '.' [0-9]* | '.' [0-9]+
        ;

STRING
    : '"' (~["\\] | '\\' .)* '"'
    | '\'' (~['\\] | '\\' .)* '\''
    ;

WS : [ \t\r\n]+ -> skip;