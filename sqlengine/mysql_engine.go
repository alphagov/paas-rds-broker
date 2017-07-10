package sqlengine

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // MySQL Driver

	"strings"

	"code.cloudfoundry.org/lager"
)

type MySQLEngine struct {
	logger     lager.Logger
	db         *sql.DB
	requireSSL bool
}

func NewMySQLEngine(logger lager.Logger) *MySQLEngine {
	return &MySQLEngine{
		logger:     logger.Session("mysql-engine"),
		requireSSL: true,
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
	username = generateUsername(bindingID)
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

	createUserStatement := "CREATE USER '" + username + "'@'%' IDENTIFIED BY '" + password + "';"
	sanitizedCreateUserStatement := "CREATE USER '" + username + "'@'%' IDENTIFIED BY 'REDACTED';"
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
	username := generateUsername(bindingID)

	dropUserStatement := "DROP USER '" + username + "'@'%';"
	d.logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	if _, err := d.db.Exec(dropUserStatement); err != nil {
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
	return fmt.Sprintf("mysql://%s:%s@%s:%d/%s?reconnect=true&useSSL=l%t&sslca=/etc/ssl/certs", username, password, address, port, dbname, d.requireSSL)
}

func (d *MySQLEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:mysql://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}

func (d *MySQLEngine) connectionString(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, address, port, dbname)
}
