grammar SelectSqlGrammar;

/* =========
   Parser rules
   ========= */

query
    : selectStatement EOF
    ;

selectStatement
    : SELECT selectList FROM tableName whereClause?
    ;

selectList
    : STAR
    | column (COMMA column)*
    ;

whereClause
    : WHERE expression
    ;

/* ---- Boolean expressions ---- */

expression
    : expression AND expression
    | expression OR expression
    | predicate
    ;

predicate
    : operand comparator operand
    | LPAREN expression RPAREN
    ;

/* ---- Operands ---- */

operand
    : MINUS operand              // unary minus
    | typedLiteral               // int32(18), float64(3.14)
    | column
    ;

/* ---- Typed literal ---- */

typedLiteral
    : typeName LPAREN literal RPAREN
    ;

typeName
    : INT32
    | INT64
    | FLOAT32
    | FLOAT64
    | STRING_T
    ;

/* ---- Comparison ---- */

comparator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

/* ---- Identifiers ---- */

column
    : IDENTIFIER
    ;

tableName
    : IDENTIFIER
    ;

/* ---- Literals ---- */

literal
    : NUMBER
    | STRING
    ;

/* =========
   Lexer rules
   ========= */

/* ---- Keywords ---- */

SELECT  : [Ss][Ee][Ll][Ee][Cc][Tt];
FROM    : [Ff][Rr][Oo][Mm];
WHERE   : [Ww][Hh][Ee][Rr][Ee];
AND     : [Aa][Nn][Dd];
OR      : [Oo][Rr];

/* ---- Type keywords ---- */

INT32    : [Ii][Nn][Tt] '32';
INT64    : [Ii][Nn][Tt] '64';
FLOAT32 : [Ff][Ll][Oo][Aa][Tt] '32';
FLOAT64 : [Ff][Ll][Oo][Aa][Tt] '64';
STRING_T: [Ss][Tt][Rr][Ii][Nn][Gg];

/* ---- Symbols ---- */

STAR    : '*';
COMMA   : ',';
LPAREN  : '(';
RPAREN  : ')';

/* ---- Operators ---- */

EQ      : '=';
NEQ     : '!=';
LTE     : '<=';
LT      : '<';
GTE     : '>=';
GT      : '>';
MINUS   : '-';

/* ---- Identifiers ---- */

IDENTIFIER
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

/* ---- Literals ---- */

NUMBER
    : [0-9]+ ('.' [0-9]+)?
    ;

STRING
    : '"' (~["\\] | '\\' .)* '"'
    | '\'' (~['\\] | '\\' .)* '\''
    ;

/* ---- Whitespace ---- */

WS
    : [ \t\r\n]+ -> skip
    ;
