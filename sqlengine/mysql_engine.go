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

// no real mysql string-escaping function is provided by a widely-used
// library and prepared statements don't seem to work with CREATE USER
// so the best we can easily do is a sanity check
func checkMySQLLiteralSafe(s string) error {
	if strings.Contains(s, "'") || strings.Contains(s, "\x00") || strings.Contains(s, "\x1a") {
		return errors.New("String " + s + " contains mysql-literal-unsafe characters")
	}

	return nil
}

// the same is doubly true for identifier escaping
func checkMySQLIdentifierSafe(s string) error {
	if strings.Contains(s, "`") || strings.Contains(s, "\x00") || strings.Contains(s, "\x1a") {
		return errors.New("String " + s + " contains mysql-identifier-unsafe characters")
	}

	return nil
}

func (d *MySQLEngine) Open(address string, port int64, dbname string, username string, password string) error {
	logger := d.logger.Session("open")
	logger.Debug("start")

	connectionString := d.connectionString(address, port, dbname, username, password)
	sanitizedConnectionString := d.connectionString(address, port, dbname, username, "REDACTED")
	logger.Debug("sql-open", lager.Data{"connection-string": sanitizedConnectionString})

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

	// let's not make sanitizing literals any more complex
	noBackslashEscapesStatement := "SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'"
	logger.Debug("sql-open", lager.Data{"statement": noBackslashEscapesStatement})
	if _, err := d.db.Exec(noBackslashEscapesStatement); err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) Close() {
	logger := d.logger.Session("close")
	logger.Debug("start")

	if d.db != nil {
		d.db.Close()
	}
}

func (d *MySQLEngine) CreateUser(bindingID, dbname string, readOnly bool) (username, password string, err error) {
	logger := d.logger.Session("create-user", lager.Data{bindingIDLogKey: bindingID})
	logger.Debug("start")

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

	if err := checkMySQLIdentifierSafe(username); err != nil {
		return "", "", err
	}
	if err := checkMySQLIdentifierSafe(dbname); err != nil {
		return "", "", err
	}
	if err := checkMySQLLiteralSafe(password); err != nil {
		return "", "", err
	}

	createUserStatement := "CREATE USER `" + username + "`@`%` IDENTIFIED BY '" + password + "'" + userRequireSSL + ";"
	sanitizedCreateUserStatement := "CREATE USER `" + username + "`@`%` IDENTIFIED BY 'REDACTED'" + userRequireSSL + ";"
	logger.Debug("create-user", lager.Data{"statement": sanitizedCreateUserStatement})

	if _, err := d.db.Exec(createUserStatement); err != nil {
		logger.Error("sql-error", err)
		return "", "", err
	}

	grantPrivilegesStatement := "GRANT " + strings.Join(options, ", ") + " ON `" + dbname + "`.* TO `" + username + "`@`%`;"
	logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.db.Exec(grantPrivilegesStatement); err != nil {
		logger.Error("sql-error", err)
		return "", "", err
	}

	return username, password, nil
}

func (d *MySQLEngine) DropUser(bindingID string) error {
	logger := d.logger.Session("drop-user", lager.Data{bindingIDLogKey: bindingID})
	logger.Debug("start")

	username := d.UsernameGenerator(bindingID)

	if err := checkMySQLIdentifierSafe(username); err != nil {
		return err
	}

	dropUserStatement := "DROP USER `" + username + "`@`%`;"
	logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	_, err := d.db.Exec(dropUserStatement)
	if err == nil {
		return nil
	}

	logger.Error("sql-error", err)

	// Try to drop the username generated the old way

	username = generateUsernameOld(bindingID)

	if err := checkMySQLIdentifierSafe(username); err != nil {
		return err
	}

	dropUserStatement = "DROP USER `" + username + "`@`%`;"
	logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	_, err = d.db.Exec(dropUserStatement)
	if err != nil {
		logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *MySQLEngine) ResetState() error {
	logger := d.logger.Session("reset-state")
	logger.Debug("start")

	// user management in mysql isn't transactional, so no point in trying
	// to do this in a transaction.
	users, err := d.listNonSuperUsers(logger)
	if err != nil {
		return err
	}

	for _, username := range users {
		if err := checkMySQLIdentifierSafe(username); err != nil {
			return err
		}

		dropUserStatement := "DROP USER `" + username + "`@`%`;"
		logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

		_, err = d.db.Exec(dropUserStatement)
		if err != nil {
			logger.Error("sql-error", err)
			return err
		}
	}

	return nil
}

func (d *MySQLEngine) listNonSuperUsers(logger lager.Logger) ([]string, error) {
	users := []string{}

	rows, err := d.db.Query(
		"SELECT User FROM mysql.user WHERE Super_priv != 'Y' AND Host = '%' AND User != SUBSTRING_INDEX(CURRENT_USER(), '@', 1)",
	)
	if err != nil {
		logger.Error("sql-error", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			logger.Error("sql-error", err)
			return nil, err
		}
		users = append(users, username)
	}
	return users, nil
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
