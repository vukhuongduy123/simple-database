grammar DropTableSqlGrammar;

query
    : dropTableStatement EOF
    ;

dropTableStatement
    : DROP TABLE tableName
    ;

tableName
    : IDENTIFIER
    ;

DROP  : [Dd][Rr][Oo][Pp];
TABLE : [Tt][Aa][Bb][Ll][Ee];

IDENTIFIER
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

WS : [ \t\r\n]+-> skip;