package sqlengine

import (
	"errors"
)

type SQLEngine interface {
	Open(address string, port int64, dbname string, username string, password string) error
	Close()
	CreateUser(username, password, dbname string) error
	DropUser(username string) error
	URI(address string, port int64, dbname string, username string, password string) string
	JDBCURI(address string, port int64, dbname string, username string, password string) string
}

var LoginFailedError = errors.New("Login failed")
