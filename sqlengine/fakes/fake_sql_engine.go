package fakes

import (
	"fmt"
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

	CreateUserCalled   bool
	CreateUserUsername string
	CreateUserPassword string
	CreateUserError    error

	DropUserCalled   bool
	DropUserUsername string
	DropUserError    error

	GrantPrivilegesCalled   bool
	GrantPrivilegesDBName   string
	GrantPrivilegesUsername string
	GrantPrivilegesError    error
}

func (f *FakeSQLEngine) Open(address string, port int64, dbname string, username string, password string) error {
	f.OpenCalled = true
	f.OpenAddress = address
	f.OpenPort = port
	f.OpenDBName = dbname
	f.OpenUsername = username
	f.OpenPassword = password

	return f.OpenError
}

func (f *FakeSQLEngine) Close() {
	f.CloseCalled = true
}

func (f *FakeSQLEngine) CreateUser(username string, password string) error {
	f.CreateUserCalled = true
	f.CreateUserUsername = username
	f.CreateUserPassword = password

	return f.CreateUserError
}

func (f *FakeSQLEngine) DropUser(username string) error {
	f.DropUserCalled = true
	f.DropUserUsername = username

	return f.DropUserError
}

func (f *FakeSQLEngine) GrantPrivileges(dbname string, username string) error {
	f.GrantPrivilegesCalled = true
	f.GrantPrivilegesDBName = dbname
	f.GrantPrivilegesUsername = username

	return f.GrantPrivilegesError
}

func (f *FakeSQLEngine) URI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("fake://%s:%s@%s:%d/%s?reconnect=true", username, password, address, port, dbname)
}

func (f *FakeSQLEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:fake://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}
