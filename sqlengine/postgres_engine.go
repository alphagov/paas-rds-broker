package sqlengine

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq" // PostgreSQL Driver

	"github.com/pivotal-golang/lager"
)

const defaultStateDBName = "broker_state"

type PostgresEngine struct {
	logger             lager.Logger
	stateEncryptionKey string
	db                 *sql.DB
	address            string
	port               int64
	username           string
	password           string
	requireSSL         bool
	stateDBName        string
}

func NewPostgresEngine(logger lager.Logger, stateEncryptionKey string) *PostgresEngine {
	return &PostgresEngine{
		logger:             logger.Session("postgres-engine"),
		stateEncryptionKey: stateEncryptionKey,
		requireSSL:         true,
		stateDBName:        defaultStateDBName,
	}
}

func (d *PostgresEngine) Open(address string, port int64, dbname string, username string, password string) error {
	d.address, d.port, d.username, d.password = address, port, username, password
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

	// Open() may not actually open the connection so we ping to validate it
	err = d.db.Ping()
	if err != nil {
		// We specifically look for invalid password error and map it to a
		// generic error that can be the same across other engines
		// See: https://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "28P01" {
			// return &LoginFailedError{username}
			return LoginFailedError
		}
		return err
	}

	return nil
}

func (d *PostgresEngine) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *PostgresEngine) CreateUser(bindingID, dbname string) (username, password string, err error) {
	stateDB, err := d.openStateDB(d.logger, d.stateEncryptionKey)
	if err != nil {
		return "", "", err
	}
	defer stateDB.Close()

	tx, err := stateDB.Begin()
	if err != nil {
		stateDB.logger.Error("sql-error", err)
		return "", "", err
	}
	commitCalled := false
	defer func() {
		if !commitCalled {
			tx.Rollback()
		}
	}()

	username = generatePostgresUsername(dbname)

	password, ok, err := stateDB.fetchUserPassword(username)
	if err != nil {
		return "", "", err
	}
	if ok {
		// User already exists. Nothing further to do.
		return username, password, nil
	}

	// No user exists, proceed with creation.

	password = generatePassword()
	var (
		createUserStatement          = "CREATE USER \"" + username + "\" WITH PASSWORD '" + password + "'"
		sanitizedCreateUserStatement = "CREATE USER \"" + username + "\" WITH PASSWORD 'REDACTED'"
	)
	d.logger.Debug("create-user", lager.Data{"statement": sanitizedCreateUserStatement})
	if _, err := tx.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	grantPrivilegesStatement := "GRANT ALL PRIVILEGES ON DATABASE \"" + dbname + "\" TO \"" + username + "\""
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})
	if _, err := tx.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	err = stateDB.storeUser(username, password)
	if err != nil {
		return "", "", err
	}
	err = tx.Commit()
	commitCalled = true // Prevent Rollback being called in deferred function
	if err != nil {
		d.logger.Error("commit.sql-error", err)
		return "", "", err
	}

	return username, password, nil
}

func (d *PostgresEngine) DropUser(bindingID string) error {
	// For PostgreSQL we don't drop the user because we retain a single user for all bound applications

	return nil
}

func (d *PostgresEngine) ResetState() error {
	stateDB, err := d.openStateDB(d.logger, d.stateEncryptionKey)
	if err != nil {
		return err
	}
	defer stateDB.Close()

	tx, err := stateDB.Begin()
	if err != nil {
		stateDB.logger.Error("sql-error", err)
		return err
	}
	commitCalled := false
	defer func() {
		if !commitCalled {
			tx.Rollback()
		}
	}()

	users, err := stateDB.listUsers()

	for _, username := range users {
		password := generatePassword()
		// User already exists but the password needs to be reset.
		var (
			updateUserStatement          = "ALTER USER \"" + username + "\" WITH PASSWORD '" + password + "'"
			sanitizedUpdateUserStatement = "ALTER USER \"" + username + "\" WITH PASSWORD 'REDACTED'"
		)
		d.logger.Debug("alter-user", lager.Data{"statement": sanitizedUpdateUserStatement})
		if _, err := tx.Exec(updateUserStatement); err != nil {
			d.logger.Error("sql-error", err)
			return err
		}
		err = stateDB.updateUser(username, password)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	commitCalled = true // Prevent Rollback being called in deferred function
	if err != nil {
		d.logger.Error("commit.sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) URI(address string, port int64, dbname string, username string, password string) string {
	uri := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", username, password, address, port, dbname)
	if !d.requireSSL {
		uri = uri + "?sslmode=disable"
	}
	return uri
}

func (d *PostgresEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?user=%s&password=%s", address, port, dbname, username, password)
}

// generatePostgresUsername produces a deterministic user name. This is because the role
// will be persisted across multiple application bindings
func generatePostgresUsername(dbname string) string {
	return dbname + "_owner"
}
