package sqlengine

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql" // MySQL Driver

	"strings"

	"code.cloudfoundry.org/lager"
)

const ER_ACCESS_DENIED_ERROR = 1045

type MySQLEngine struct {
	logger            lager.Logger
	db                *sql.DB
	requireSSL        bool
	UsernameGenerator func(string) string
}

func NewMySQLEngine(logger lager.Logger) *MySQLEngine {
	return &MySQLEngine{
		logger:            logger.Session("mysql-engine"),
		requireSSL:        true,
		UsernameGenerator: generateUsername,
	}
}

func (d *MySQLEngine) Open(address string, port int64, dbname string, username string, password string) error {
	connectionString := d.connectionString(address, port, dbname, username, password)
	sanitizedConnectionString := d.connectionString(address, port, dbname, username, "REDACTED")
	d.logger.Debug("sql-open", lager.Data{"connection-string": sanitizedConnectionString})

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}

	d.db = db

	// Open() may not actually open the connection so we ping to validate it
	err = d.db.Ping()
	if err != nil {
		// We specifically look for invalid password error and map it to a
		// generic error that can be the same across other engines
		// See: https://github.com/VividCortex/mysqlerr/blob/master/mysqlerr.go
		if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == ER_ACCESS_DENIED_ERROR {
			return LoginFailedError
		}
		return err
	}

	return nil
}

func (d *MySQLEngine) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *MySQLEngine) CreateUser(bindingID, dbname string) (username, password string, err error) {
	username = d.UsernameGenerator(bindingID)
	password = generatePassword()
	options := []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"CREATE",
		"DROP",
		"REFERENCES",
		"INDEX",
		"ALTER",
		"CREATE TEMPORARY TABLES",
		"LOCK TABLES",
		"EXECUTE",
		"CREATE VIEW",
		"SHOW VIEW",
		"CREATE ROUTINE",
		"ALTER ROUTINE",
		"EVENT",
		"TRIGGER",
	}

	var userRequireSSL string
	if d.requireSSL {
		userRequireSSL = " REQUIRE SSL"
	}

	createUserStatement := "CREATE USER '" + username + "'@'%' IDENTIFIED BY '" + password + "'" + userRequireSSL + ";"
	sanitizedCreateUserStatement := "CREATE USER '" + username + "'@'%' IDENTIFIED BY 'REDACTED'" + userRequireSSL + ";"
	d.logger.Debug("create-user", lager.Data{"statement": sanitizedCreateUserStatement})

	if _, err := d.db.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	grantPrivilegesStatement := "GRANT " + strings.Join(options, ", ") + " ON `" + dbname + "`.* TO '" + username + "'@'%';"
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.db.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	return username, password, nil
}

func (d *MySQLEngine) DropUser(bindingID string) error {
	username := d.UsernameGenerator(bindingID)

	dropUserStatement := "DROP USER '" + username + "'@'%';"
	d.logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	_, err := d.db.Exec(dropUserStatement)
	if err == nil {
		return nil
	}

	d.logger.Error("sql-error", err)

	// Try to drop the username generated the old way

	username = generateUsernameOld(bindingID)

	dropUserStatement = "DROP USER '" + username + "'@'%';"
	d.logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	_, err = d.db.Exec(dropUserStatement)
	if err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) ResetState() error {
	// TODO: Not implemented
	return errors.New("Not implemented")
}

func (d *MySQLEngine) URI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("mysql://%s:%s@%s:%d/%s?reconnect=true&useSSL=%t", username, password, address, port, dbname, d.requireSSL)
}

func (d *MySQLEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:mysql://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}

func (d *MySQLEngine) connectionString(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, address, port, dbname)
}

func (d *MySQLEngine) CreateExtensions(extensions []string) error {
	return nil
}

func (d *MySQLEngine) DropExtensions(extensions []string) error {
	return nil
}

func (d *MySQLEngine) ExecuteStatement(statement string) error {
	return nil
}

func (d *MySQLEngine) CreateSchema(schemaname string) error {
	return nil
}

func (d *MySQLEngine) DropSchema(schemaname string) error {
	return nil
}

func (d *MySQLEngine) GrantPrivileges(alterPrivileges bool, schemaName string, grantType string, grantOn string, roleName string) error {
	return nil
}

func (d *MySQLEngine) RevokePrivileges(alterPrivileges bool, schemaName string, grantType string, grantOn string, roleName string) error {
	return nil
}
