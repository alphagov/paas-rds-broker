package sqlengine

import (
	"encoding/json"
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
	CreateUser(bindingID, dbname string, userBindParametersRaw *json.RawMessage) (string, string, error)
	DropUser(bindingID string) error
	ResetState() error
	URI(address string, port int64, dbname string, username string, password string) string
	JDBCURI(address string, port int64, dbname string, username string, password string) string
	CreateExtensions(extensions []string) error
	DropExtensions(extensions []string) error
}

var LoginFailedError = errors.New("Login failed")

func generateUsername(seed string) string {
	usernameString := strings.ToLower(utils.GenerateHash(seed, usernameLength-1))
	return "u" + strings.Replace(usernameString, "-", "_", -1)
}

func generateUsernameOld(seed string) string {
	usernameString := strings.ToLower(utils.GetMD5B64(seed, usernameLength-1))
	return "u" + strings.Replace(usernameString, "-", "_", -1)
}

func generatePassword() string {
	return utils.RandomAlphaNum(passwordLength)
}
