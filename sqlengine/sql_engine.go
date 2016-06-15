package sqlengine

import (
	"errors"
	"strings"

	"github.com/alphagov/paas-rds-broker/utils"
)

const (
	usernameLength = 16
	passwordLength = 32
)

type SQLEngine interface {
	Open(address string, port int64, dbname string, username string, password string) error
	Close()
	CreateUser(bindingID, dbname string) (string, string, error)
	DropUser(bindingID string) error
	URI(address string, port int64, dbname string, username string, password string) string
	JDBCURI(address string, port int64, dbname string, username string, password string) string
}

var LoginFailedError = errors.New("Login failed")

func generateUsername(seed string) string {
	return "u" + strings.Replace(utils.GetMD5B64(seed, usernameLength-1), "-", "_", -1)
}

func generatePassword() string {
	return utils.RandomAlphaNum(passwordLength)
}
