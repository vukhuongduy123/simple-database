# Detect OS
ifeq ($(OS),Windows_NT)
    APP := app.exe
    RM_DIR := rmdir /S /Q
    MKDIR  := mkdir
else
    APP := app
    RM_DIR := rm -rf
    MKDIR  := mkdir -p
endif

CMD := ./cmd/api
OUT_DIR := out

.PHONY: all grammar build clean

all: grammar build

ifeq ($(OS),Windows_NT)

grammar:
	antlr4 -Dlanguage=Go -visitor -o .\internal\parser\grammar\create .\configs\CreateTableSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o .\internal\parser\grammar\delete .\configs\DeleteSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o .\internal\parser\grammar\drop   .\configs\DropTableSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o .\internal\parser\grammar\insert .\configs\InsertSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o .\internal\parser\grammar\select .\configs\SelectSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o .\internal\parser\grammar\update .\configs\UpdateSqlGrammar.g4
else

grammar:
	antlr4 -Dlanguage=Go -visitor -o ./internal/parser/grammar/create ./configs/CreateTableSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o ./internal/parser/grammar/delete ./configs/DeleteSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o ./internal/parser/grammar/drop   ./configs/DropTableSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o ./internal/parser/grammar/insert ./configs/InsertSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o ./internal/parser/grammar/select ./configs/SelectSqlGrammar.g4
	antlr4 -Dlanguage=Go -visitor -o ./internal/parser/grammar/update ./configs/UpdateSqlGrammar.g4

endif

build:
	$(MKDIR) $(OUT_DIR)
	go build -o $(OUT_DIR)/$(APP) $(CMD)

clean:
	-$(RM_DIR) $(OUT_DIR)