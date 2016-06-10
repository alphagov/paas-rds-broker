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
	dropUserStatement := "DROP USER \"" + username + "\""
	d.logger.Debug("drop-user", lager.Data{"statement": dropUserStatement})

	if _, err := d.db.Exec(dropUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) GrantPrivileges(dbname string, username string) error {
	groupName := dbname + "_owner"
	err := d.setupGroup(dbname, groupName)
	if err != nil {
		return err
	}

	grantRoleStatement := "GRANT \"" + groupName + "\" TO \"" + username + "\""
	d.logger.Debug("grant-role", lager.Data{"statement": grantRoleStatement})

	if _, err := d.db.Exec(grantRoleStatement); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) setupGroup(dbname, groupName string) error {
	statements := []string{
		// Create group if it doesn't exist.
		`DO
$body$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + groupName + `') THEN
        CREATE GROUP "` + groupName + `";
    END IF;
END
$body$;`,

		// Allow access to the database
		"GRANT ALL PRIVILEGES ON DATABASE \"" + dbname + "\" TO \"" + groupName + "\"",

		// Create/replace function to set the owner of new objects to the group
		`CREATE OR REPLACE FUNCTION change_owner() RETURNS event_trigger AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
        REASSIGN OWNED BY current_user TO ` + groupName + `;
    END IF;
END;
$$ LANGUAGE plpgsql;`,

		// Create the trigger to call the above function
		"DROP EVENT TRIGGER IF EXISTS change_owner;",
		"CREATE EVENT TRIGGER change_owner ON ddl_command_end EXECUTE PROCEDURE change_owner();",
	}

	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	for _, statement := range statements {
		d.logger.Debug("grant-privileges", lager.Data{"statement": statement})
		if _, err := tx.Exec(statement); err != nil {
			d.logger.Error("sql-error", err)
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
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
