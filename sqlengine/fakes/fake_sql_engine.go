package fakes

import (
	"fmt"

	"github.com/alphagov/paas-rds-broker/sqlengine"
)

type FakeSQLEngine struct {
	OpenCalled   bool
	OpenAddress  string
	OpenPort     int64
	OpenDBName   string
	OpenUsername string
	OpenPassword string
	OpenError    error

	CloseCalled bool

	CreateUserCalled    bool
	CreateUserBindingID string
	CreateUserDBName    string
	// returns
	CreateUserUsername string
	CreateUserPassword string
	CreateUserError    error

	DropUserCalled    bool
	DropUserBindingID string
	DropUserError     error

	CreateExtensionsCalled bool

	ResetStateCalled bool
	ResetStateError  error

	CorrectPassword string
}

func (f *FakeSQLEngine) Open(address string, port int64, dbname string, username string, password string) error {
	f.OpenCalled = true
	f.OpenAddress = address
	f.OpenPort = port
	f.OpenDBName = dbname
	f.OpenUsername = username
	f.OpenPassword = password

	if f.OpenError == nil {
		if f.CorrectPassword != "" && f.CorrectPassword != password {
			return sqlengine.LoginFailedError
		}
	}
	return f.OpenError
}

func (f *FakeSQLEngine) Close() {
	f.CloseCalled = true
}

func (f *FakeSQLEngine) CreateUser(bindingID, dbname string) (username, password string, err error) {
	f.CreateUserCalled = true
	f.CreateUserBindingID = bindingID
	f.CreateUserDBName = dbname

	return f.CreateUserUsername, f.CreateUserPassword, f.CreateUserError
}

func (f *FakeSQLEngine) DropUser(bindingID string) error {
	f.DropUserCalled = true
	f.DropUserBindingID = bindingID

	return f.DropUserError
}

func (f *FakeSQLEngine) ResetState() error {
	f.ResetStateCalled = true

	return f.ResetStateError
}

func (f *FakeSQLEngine) URI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("fake://%s:%s@%s:%d/%s?reconnect=true", username, password, address, port, dbname)
}

func (f *FakeSQLEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:fake://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}

func (f *FakeSQLEngine) CreateExtensions(extensions []string) error {
	f.CreateExtensionsCalled = true

	return nil
}
