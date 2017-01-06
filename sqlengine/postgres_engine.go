package sqlengine

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/lib/pq" // PostgreSQL Driver
	"github.com/pivotal-golang/lager"
)

var ensureGroupPattern = `
do
$body$
begin
	if not exists (select * from pg_catalog.pg_roles where rolname = '{{.role}}') then
		create role "{{.role}}";
	end if;
	grant all privileges on database "{{.database}}" to "{{.role}}";
end
$body$
`
var ensureTriggerPattern = `
create or replace function reassign_owned() returns event_trigger language plpgsql as $$
begin
	if pg_has_role(current_user, '{{.role}}', 'member') then
		execute 'reassign owned by "' || current_user || '" to "{{.role}}"';
	end if;
end
$$;
`

var ensureTriggerTemplate = template.Must(template.New("ensureTrigger").Parse(ensureTriggerPattern))
var ensureGroupTemplate = template.Must(template.New("ensureGroup").Parse(ensureGroupPattern))

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
	connectionString := d.URI(address, port, dbname, username, password)
	sanitizedConnectionString := d.URI(address, port, dbname, username, "REDACTED")
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
	groupname := d.generatePostgresGroup(dbname)

	if err := d.ensureTrigger(dbname, groupname); err != nil {
		return "", "", err
	}

	username = generateUsername(bindingID)
	password = generatePassword()

	createUserStatement := fmt.Sprintf(`create role "%s" inherit login password '%s'`, username, password)
	d.logger.Debug("create-user", lager.Data{"statement": createUserStatement})

	if _, err := d.db.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	grantPrivilegesStatement := fmt.Sprintf(`grant "%s" to "%s"`, groupname, username)
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.db.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	return username, password, nil
}

func (d *PostgresEngine) DropUser(bindingID string) error {
	username := generateUsername(bindingID)
	dropUserStatement := fmt.Sprintf(`drop role "%s"`, username)

	if _, err := d.db.Exec(dropUserStatement); err != nil {
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

// generatePostgresGroup produces a deterministic group name. This is because the role
// will be persisted across all application bindings
func (d *PostgresEngine) generatePostgresGroup(dbname string) string {
	return dbname + "_manager"
}

func (d *PostgresEngine) ensureTrigger(dbname, groupname string) error {
	var ensureGroupStatement bytes.Buffer
	if err := ensureGroupTemplate.Execute(&ensureGroupStatement, map[string]string{
		"role":     groupname,
		"database": dbname,
	}); err != nil {
		return err
	}

	var ensureTriggerStatement bytes.Buffer
	if err := ensureTriggerTemplate.Execute(&ensureTriggerStatement, map[string]string{
		"role": groupname,
	}); err != nil {
		return err
	}

	statements := []string{
		ensureGroupStatement.String(),
		ensureTriggerStatement.String(),
		`drop event trigger if exists reassign_owned;`,
		`create event trigger reassign_owned on ddl_command_end execute procedure reassign_owned();`,
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	commitCalled := false
	defer func() {
		if !commitCalled {
			tx.Rollback()
		}
	}()

	for _, statement := range statements {
		d.logger.Debug("ensure-trigger", lager.Data{"statement": statement})
		_, err = tx.Exec(statement)
		if err != nil {
			d.logger.Error("sql-error", err)
			return err
		}
	}

	err = tx.Commit()
	commitCalled = true
	if err != nil {
		d.logger.Error("commit.sql-error", err)
		return err
	}

	return nil
}
