package sqlengine

import (
	"database/sql"

	"github.com/lib/pq"
	"github.com/pivotal-golang/lager"
)

// passwordStorageVersion represents the current method we are using to store
// passwords. If the way we store or encrypt passwords ever changes, this field
// (which is stored in the database) will allow us to migrate more easily.
const passwordStorageVersion = "1.0"

func (d *PostgresEngine) openStateDB(logger lager.Logger, stateEncryptionKey string) (*postgresEngineState, error) {
	logger = logger.Session("postgres-engine-state")

	statement := "CREATE DATABASE " + d.stateDBName
	logger.Debug("create-database", lager.Data{"statement": statement})
	_, err := d.db.Exec(statement)
	if err != nil {
		// 42P04 means duplicate database - https://www.postgresql.org/docs/9.5/static/errcodes-appendix.html
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42P04" {
			// Database already exists. Carry on.
		} else {
			logger.Error("create-database.sql-error", err)
			return nil, err
		}
	} else {
		// No error, so the database has just been created
		revokePrivilegesStatement := "REVOKE ALL PRIVILEGES ON DATABASE " + d.stateDBName + " FROM PUBLIC"
		logger.Debug("revoke-privileges", lager.Data{"statement": revokePrivilegesStatement})
		if _, err := d.db.Exec(revokePrivilegesStatement); err != nil {
			logger.Error("revoke-privileges.sql-error", err)
			return nil, err
		}
	}

	var (
		stateDBURL          = d.URI(d.address, d.port, d.stateDBName, d.username, d.password)
		sanitisedStateDBURL = d.URI(d.address, d.port, d.stateDBName, d.username, "REDACTED")
	)
	logger.Debug("db-open", lager.Data{"connection-string": sanitisedStateDBURL})
	db, err := sql.Open("postgres", stateDBURL)
	if err != nil {
		logger.Error("db-open.sql-error", err)
		return nil, err
	}

	s := &postgresEngineState{
		DB:                 db,
		logger:             logger,
		stateEncryptionKey: stateEncryptionKey,
	}

	err = s.initSchema()
	if err != nil {
		s.Close()
		return nil, err
	}

	return s, nil
}

type postgresEngineState struct {
	*sql.DB
	logger             lager.Logger
	stateEncryptionKey string
}

func (s *postgresEngineState) initSchema() error {
	statement := "CREATE TABLE IF NOT EXISTS role (username varchar(128) NOT NULL, encrypted_password varchar(128) NOT NULL, password_storage_version varchar(10), PRIMARY KEY(username))"
	s.logger.Debug("create-table", lager.Data{"statement": statement})
	_, err := s.Exec(statement)
	if err != nil {
		s.logger.Error("create-table.sql-error", err)
		return err
	}
	return nil
}

func (s *postgresEngineState) fetchUserPassword(username string) (password string, ok bool, err error) {
	var encryptedPassword string
	statement := "SELECT encrypted_password FROM role WHERE username = $1"
	s.logger.Debug("fetch-user", lager.Data{"statement": statement, "params": []string{username}})
	err = s.QueryRow(statement, username).Scan(&encryptedPassword)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		s.logger.Error("fetch-user.sql-error", err)
		return "", false, err
	}
	password, err = decryptString(s.stateEncryptionKey, encryptedPassword)
	return password, (err == nil), err
}

func (s *postgresEngineState) storeUser(username, password string) error {
	encryptedPassword, err := encryptString(s.stateEncryptionKey, password)
	if err != nil {
		return err
	}
	statement := "INSERT INTO role (username, encrypted_password, password_storage_version) VALUES($1, $2, $3)"
	s.logger.Debug("insert-user", lager.Data{
		"statement": statement,
		"params":    []string{username, "REDACTED", passwordStorageVersion},
	})
	_, err = s.Exec(statement, username, encryptedPassword, passwordStorageVersion)
	if err != nil {
		s.logger.Error("insert-user.sql-error", err)
		return err
	}
	return nil
}
