package sqlengine

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL Driver

	"github.com/pivotal-golang/lager"
)

type PostgresEngine struct {
	logger lager.Logger
	db     *sql.DB
}

func NewPostgresEngine(logger lager.Logger) *PostgresEngine {
	return &PostgresEngine{
		logger: logger.Session("postgres-engine"),
	}
}

func (d *PostgresEngine) Open(address string, port int64, dbname string, username string, password string) error {
	var (
		connectionString          = d.URI(address, port, dbname, username, password)
		sanitizedConnectionString = d.URI(address, port, dbname, username, "REDACTED")
	)
	d.logger.Debug("sql-open", lager.Data{"connection-string": sanitizedConnectionString})

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	d.db = db

	return nil
}

func (d *PostgresEngine) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *PostgresEngine) CreateUser(username string, password string) error {
	var (
		createUserStatement          = "CREATE USER \"" + username + "\" WITH PASSWORD '" + password + "'"
		sanitizedCreateUserStatement = "CREATE USER \"" + username + "\" WITH PASSWORD 'REDACTED'"
	)
	d.logger.Debug("create-user", lager.Data{"statement": sanitizedCreateUserStatement})

	if _, err := d.db.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) DropUser(username string) error {
	// For PostgreSQL we don't drop the user because it might still be owner of some objects

	return nil
}

func (d *PostgresEngine) GrantPrivileges(dbname string, username string) error {
	grantPrivilegesStatement := "GRANT ALL PRIVILEGES ON DATABASE \"" + dbname + "\" TO \"" + username + "\""
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.db.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) URI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", username, password, address, port, dbname)
}

func (d *PostgresEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}
