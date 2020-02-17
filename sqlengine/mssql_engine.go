package sqlengine

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"text/template"

	// MSSQL Driver

	"code.cloudfoundry.org/lager"
	"github.com/lib/pq"
)

type MSSQLEngine struct {
	logger            lager.Logger
	db                *sql.DB
	requireSSL        bool
	UsernameGenerator func(string) string
}

func NewMSSQLEngine(logger lager.Logger) *MSSQLEngine {
	return &MSSQLEngine{
		logger:            logger.Session("mssql-engine"),
		requireSSL:        true,
		UsernameGenerator: generateUsername,
	}
}

func (d *MSSQLEngine) Open(address string, port int64, dbname string, username string, password string) error {
	connectionString := d.URI(address, port, dbname, username, password)
	sanitizedConnectionString := d.URI(address, port, dbname, username, "REDACTED")
	d.logger.Debug("sql-open", lager.Data{"connection-string": sanitizedConnectionString})

	db, err := sql.Open("mssql", connectionString)
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

func (d *MSSQLEngine) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *MSSQLEngine) execCreateUser(tx *sql.Tx, bindingID, dbname string) (username, password string, err error) {
	username = d.UsernameGenerator(bindingID)
	password = generatePassword()

	if err = d.ensureUser(tx, dbname, username, password); err != nil {
		return "", "", err
	}

	return username, password, nil
}

func (d *MSSQLEngine) createUser(bindingID, dbname string) (username, password string, err error) {
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}
	username, password, err = d.execCreateUser(tx, bindingID, dbname)
	if err != nil {
		_ = tx.Rollback()
		return "", "", err
	}
	return username, password, tx.Commit()
}

func (d *MSSQLEngine) CreateUser(bindingID, dbname string) (username, password string, err error) {
	tries := 0
	for tries < 10 {
		tries++
		username, password, err := d.createUser(bindingID, dbname)
		if err != nil {
			return "", "", err
		}
		return username, password, nil
	}
	return "", "", nil

}

func (d *MSSQLEngine) DropUser(bindingID string) error {
	username := d.UsernameGenerator(bindingID)
	dropUserStatement := fmt.Sprintf(`drop role "%s"`, username)

	_, err := d.db.Exec(dropUserStatement)
	if err == nil {
		return nil
	}

	// When handling unbinds for bindings created before the switch to
	// event-triggers based permissions the `username` won't exist.
	// Also we changed how we generate usernames so we have to try to drop the username generated
	// the old way. If none of the usernames exist then we swallow the error
	if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42704" {
		d.logger.Info("warning", lager.Data{"warning": "User " + username + " does not exist"})

		username = generateUsernameOld(bindingID)
		dropUserStatement = fmt.Sprintf(`drop role "%s"`, username)
		if _, err = d.db.Exec(dropUserStatement); err != nil {
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42704" {
				d.logger.Info("warning", lager.Data{"warning": "User " + username + " does not exist"})
				return nil
			}
			d.logger.Error("sql-error", err)
			return err
		}

		return nil
	}

	d.logger.Error("sql-error", err)

	return err
}

func (d *MSSQLEngine) ResetState() error {
	return nil
}

func (d *MSSQLEngine) listNonSuperUsers() ([]string, error) {
	users := []string{}

	rows, err := d.db.Query("select usename from pg_user where usesuper != true and usename != current_user")
	if err != nil {
		d.logger.Error("sql-error", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			d.logger.Error("sql-error", err)
			return nil, err
		}
		users = append(users, username)
	}
	return users, nil
}

func (d *MSSQLEngine) URI(address string, port int64, dbname string, username string, password string) string {
	uri := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", username, password, address, port, dbname)
	if !d.requireSSL {
		uri = uri + "&integratedSecurity=true"
	}
	return uri
}

func (d *MSSQLEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	params := []string{
		fmt.Sprintf("instanceName=%s", dbname),
		fmt.Sprintf("user=%s", username),
		fmt.Sprintf("password=%s", password),
	}

	if d.requireSSL {
		params = append(params, "integratedSecurity=true")
	}

	return fmt.Sprintf("jdbc:sqlserver://%s:%d;%s;", address, port, strings.Join(params, ";"))
}

func (d *MSSQLEngine) CreateExtensions(extensions []string) error {
	return nil
}

func (d *MSSQLEngine) DropExtensions(extensions []string) error {
	return nil
}

func (d *MSSQLEngine) ensureUser(tx *sql.Tx, dbname string, username string, password string) error {
	const ensureCreateUserPattern = `CREATE USER {{.user}} WITH PASSWORD '{{.password}}';`

	var ensureCreateUserTemplate = template.Must(template.New("ensureUser").Parse(ensureCreateUserPattern))

	var ensureUserStatement bytes.Buffer
	if err := ensureCreateUserTemplate.Execute(&ensureUserStatement, map[string]string{
		"password": password,
		"user":     username,
	}); err != nil {
		return err
	}
	var ensureUserStatementSanitized bytes.Buffer
	if err := ensureCreateUserTemplate.Execute(&ensureUserStatementSanitized, map[string]string{
		"password": "REDACTED",
		"user":     username,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-user", lager.Data{"statement": ensureUserStatementSanitized.String()})

	if _, err := tx.Exec(ensureUserStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}
