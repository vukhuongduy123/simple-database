package commandhandler

import (
	"fmt"
	"simple-database/internal/engine"
	"simple-database/internal/parser"
	platformerror "simple-database/internal/platform/error"
	"simple-database/internal/platform/helper"
	"strings"
	"sync"
)

type SqlCommandHandler interface {
	Execute(sql string) (interface{}, error)
}

type sqlCommandHandler struct {
	db   *engine.Database
	lock *sync.RWMutex
}

func (h *sqlCommandHandler) Execute(sql string) (interface{}, error) {
	normalized := strings.ToUpper(strings.TrimSpace(sql))
	helper.Log.Debugf("Executing command: %s", sql)

	switch {
	case strings.HasPrefix(normalized, "INSERT"):
		h.lock.Lock()
		defer h.lock.Unlock()
		command, err := parser.ParseInsert(sql)
		if err != nil {
			return nil, err
		}
		return h.db.Tables[command.TableName].Insert(command)
	case strings.HasPrefix(normalized, "UPDATE"):
		h.lock.Lock()
		defer h.lock.Unlock()
		command, err := parser.ParseUpdate(sql)
		if err != nil {
			return nil, err
		}
		return h.db.Tables[command.TableName].Update(command)
	case strings.HasPrefix(normalized, "DELETE"):
		h.lock.Lock()
		defer h.lock.Unlock()
		command, err := parser.ParseDelete(sql)
		if err != nil {
			return nil, err
		}
		return h.db.Tables[command.TableName].Delete(command)
	case strings.HasPrefix(normalized, "SELECT"):
		h.lock.RLock()
		defer h.lock.RUnlock()
		command, err := parser.ParseSelect(sql)
		if err != nil {
			return nil, err
		}
		return h.db.Tables[command.TableName].Select(command)
	case strings.HasPrefix(normalized, "DROP TABLE"):
		h.lock.Lock()
		defer h.lock.Unlock()
		command, err := parser.ParseDropTable(sql)
		if err != nil {
			return nil, err
		}
		return nil, h.db.DropTable(command)
	case strings.HasPrefix(normalized, "CREATE TABLE"):
		h.lock.Lock()
		defer h.lock.Unlock()
		command, err := parser.ParseCreateTable(sql)
		if err != nil {
			return nil, err
		}
		return h.db.CreateTable(command)
	default:
		return nil, platformerror.NewStackTraceError(fmt.Sprintf("Unknown command %s", sql), platformerror.UnknownCommandErrorCode)
	}

}

func newSqlCommandHandler() (SqlCommandHandler, error) {
	db, err := engine.NewDatabase("my_db")
	if err != nil {
		panic(err)
	}
	return &sqlCommandHandler{db: db, lock: &sync.RWMutex{}}, nil
}

var (
	instance SqlCommandHandler
	once     sync.Once
	initErr  error
)

func GetSqlCommandHandler() (SqlCommandHandler, error) {
	once.Do(func() { instance, initErr = newSqlCommandHandler() })
	return instance, initErr
}
