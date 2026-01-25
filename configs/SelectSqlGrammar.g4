grammar SelectSqlGrammar;

/* =========
   Parser rules
   ========= */

query
    : selectStatement EOF
    ;

selectStatement
    : SELECT selectList FROM tableName whereClause? limitClause?
    ;

selectList
    : STAR
    | column (COMMA column)*
    ;

whereClause
    : WHERE expression
    ;

limitClause
     : LIMIT INTEGER
     ;


expression
    : expression AND expression
    | expression OR expression
    | predicate
    ;

predicate
    : operand comparator operand
    | LPAREN expression RPAREN
    ;

operand
    : MINUS operand
    | typedLiteral
    | column
    ;

typedLiteral
    : typeName LPAREN literal RPAREN
    ;

typeName
    : INT32 | INT64 | FLOAT32 | FLOAT64 | STRING_T
    ;

comparator
    : EQ | NEQ | LTE | LT | GTE | GT
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

SELECT  : [Ss][Ee][Ll][Ee][Cc][Tt];
FROM    : [Ff][Rr][Oo][Mm];
WHERE   : [Ww][Hh][Ee][Rr][Ee];
AND     : [Aa][Nn][Dd];
OR      : [Oo][Rr];
LIMIT   : [Ll][Ii][Mm][Ii][Tt];

INT32    : [Ii][Nn][Tt] '32';
INT64    : [Ii][Nn][Tt] '64';
FLOAT32  : [Ff][Ll][Oo][Aa][Tt] '32';
FLOAT64  : [Ff][Ll][Oo][Aa][Tt] '64';
STRING_T : [Ss][Tt][Rr][Ii][Nn][Gg];

STAR    : '*';
COMMA   : ',';
LPAREN  : '(';
RPAREN  : ')';

EQ      : '=';
NEQ     : '!=';
LTE     : '<=';
LT      : '<';
GTE     : '>=';
GT      : '>';
MINUS   : '-';

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