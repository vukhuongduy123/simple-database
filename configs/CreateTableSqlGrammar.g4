grammar CreateTableSqlGrammar;

query
    : createTableStatement EOF
    ;

createTableStatement
    : CREATE TABLE tableName LPAREN columnExpression(COMMA columnExpression)* RPAREN
    ;

tableName
    : IDENTIFIER
    ;

columnExpression
    : column columnDefinition
    ;

column
    : IDENTIFIER
    ;

columnDefinition
    : typeName indexType?
    ;

indexType
    : UNIQUE | INDEX | PRIMARY KEY
    ;

typeName
    : INT32 | INT64 | FLOAT32 | FLOAT64 | STRING_T
    ;

IDENTIFIER
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

CREATE     : [Cc][Rr][Ee][Aa][Tt][Ee];
TABLE      : [Tt][Aa][Bb][Ll][Ee];
COMMA      : ',';
LPAREN     : '(';
RPAREN     : ')';
UNIQUE     : [Uu][Nn][Ii][Qq][Uu][Ee];
INDEX      : [Ii][Nn][Dd][Ee][Xx];
PRIMARY    : [Pp][Rr][Ii][Mm][Aa][Rr][Yy];
KEY        : [Kk][Ee][Yy];

INT32    : [Ii][Nn][Tt] '32';
INT64    : [Ii][Nn][Tt] '64';
FLOAT32  : [Ff][Ll][Oo][Aa][Tt] '32';
FLOAT64  : [Ff][Ll][Oo][Aa][Tt] '64';
STRING_T : [Ss][Tt][Rr][Ii][Nn][Gg];

WS : [ \t\r\n]+ -> skip;