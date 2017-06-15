package sqlengine

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/lib/pq" // PostgreSQL Driver

	"code.cloudfoundry.org/lager"
	"github.com/alphagov/paas-rds-broker/awsrds"
)

var ensureTriggerPattern = `
create or replace function reassign_owned() returns event_trigger language plpgsql as $$
begin
	IF pg_has_role(current_user, '{{.role}}', 'member') AND
	   NOT EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true)
	THEN
		execute 'reassign owned by "' || current_user || '" to "{{.role}}"';
	end if;
end
$$;
`
var ensureGroupPattern = `
do
$body$
begin
	if not exists (select 1 from pg_catalog.pg_roles where rolname = '{{.role}}') then
		create role "{{.role}}";
	end if;
end
$body$
`

var ensureTriggerTemplate = template.Must(template.New("ensureTrigger").Parse(ensureTriggerPattern))
var ensureGroupTemplate = template.Must(template.New("ensureGroup").Parse(ensureGroupPattern))

const masterPasswordLength = 32

type PostgresEngine struct {
	logger     lager.Logger
	DB         *sql.DB
	requireSSL bool
}

func NewPostgresEngine(logger lager.Logger) *PostgresEngine {
	return &PostgresEngine{
		logger:     logger.Session("postgres-engine"),
		requireSSL: true,
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

	d.DB = db

	// Open() may not actually open the connection so we ping to validate it
	err = d.DB.Ping()
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
	if d.DB != nil {
		d.DB.Close()
	}
}

func (d *PostgresEngine) CreateUser(bindingID, dbname string) (username, password string, err error) {
	groupname := d.GeneratePostgresGroup(dbname)

	if err := d.EnsureGroup(dbname, groupname); err != nil {
		return "", "", err
	}

	if err := d.EnsureTrigger(groupname); err != nil {
		return "", "", err
	}

	username = generateUsername(bindingID)
	password = generatePassword()

	createUserStatement := fmt.Sprintf(`create role "%s" inherit login password '%s'`, username, password)
	d.logger.Debug("create-user", lager.Data{"statement": createUserStatement})

	if _, err := d.DB.Exec(createUserStatement); err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	users, err := d.ListNonSuperUsers()
	if err != nil {
		return "", "", err
	}

	// FIXME: Simplify when old bindings are not used anymore
	for _, user := range users {
		grantPrivilegesStatement := fmt.Sprintf(`grant "%s" to "%s"`, groupname, user)
		d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

		if _, err := d.DB.Exec(grantPrivilegesStatement); err != nil {
			d.logger.Error("sql-error", err)
			return "", "", err
		}

		reassignStatement := fmt.Sprintf(`reassign owned by "%s" to "%s"`, user, groupname)
		d.logger.Debug("reassign-objects", lager.Data{"statement": reassignStatement})

		if _, err := d.DB.Exec(reassignStatement); err != nil {
			d.logger.Error("sql-error", err)
			return "", "", err
		}

	}

	return username, password, nil
}

func (d *PostgresEngine) DropUser(bindingID string) error {
	username := generateUsername(bindingID)
	dropUserStatement := fmt.Sprintf(`drop role "%s"`, username)

	if _, err := d.DB.Exec(dropUserStatement); err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42704" {
			d.logger.Info("warning", lager.Data{"warning": "User " + username + " does not exist"})
			return nil
		}
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) ResetState() error {
	d.logger.Debug("reset-state.start")

	tx, err := d.DB.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return err
	}
	commitCalled := false
	defer func() {
		if !commitCalled {
			tx.Rollback()
		}
	}()

	users, err := d.ListNonSuperUsers()
	if err != nil {
		return err
	}

	for _, username := range users {
		dropUserStatement := fmt.Sprintf(`drop role "%s"`, username)
		d.logger.Debug("reset-state", lager.Data{"statement": dropUserStatement})
		if _, err = tx.Exec(dropUserStatement); err != nil {
			d.logger.Error("sql-error", err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		d.logger.Error("commit.sql-error", err)
		return err
	}
	commitCalled = true // Prevent Rollback being called in deferred function

	d.logger.Debug("reset-state.finish")

	return nil
}

func (d *PostgresEngine) ListNonSuperUsers() ([]string, error) {
	users := []string{}

	rows, err := d.DB.Query("select usename from pg_user where usesuper != true and usename != current_user")
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

// generatePostgresGroup produces a deterministic group name. This is because the role
// will be persisted across all application bindings
func (d *PostgresEngine) GeneratePostgresGroup(dbname string) string {
	return dbname + "_manager"
}

func (d *PostgresEngine) EnsureGroup(dbname, groupname string) error {
	var ensureGroupStatement bytes.Buffer
	if err := ensureGroupTemplate.Execute(&ensureGroupStatement, map[string]string{
		"role": groupname,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-group", lager.Data{"statement": ensureGroupStatement.String()})

	if _, err := d.DB.Exec(ensureGroupStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

func (d *PostgresEngine) EnsureTrigger(groupname string) error {
	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}

	commitCalled := false
	defer func() {
		if !commitCalled {
			tx.Rollback()
		}
	}()

	var ensureTriggerStatement bytes.Buffer
	if err := ensureTriggerTemplate.Execute(&ensureTriggerStatement, map[string]string{
		"role": groupname,
	}); err != nil {
		return err
	}

	cmds := []string{
		ensureTriggerStatement.String(),
		`drop event trigger if exists reassign_owned;`,
		`create event trigger reassign_owned on ddl_command_end execute procedure reassign_owned();`,
	}

	for _, cmd := range cmds {
		d.logger.Debug("ensure-trigger", lager.Data{"statement": cmd})
		_, err = tx.Exec(cmd)
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

func (d *PostgresEngine) Migrate(dbDetails *awsrds.DBInstanceDetails, dbname string, masterPassword string) {

	d.Open(dbDetails.Address, dbDetails.Port, dbname, dbDetails.MasterUsername, masterPassword)

	groupname := d.GeneratePostgresGroup(dbname)

	if err := d.EnsureGroup(dbname, groupname); err != nil {
		d.logger.Error("Ensure Group", err)
		return
	}

	if err := d.EnsureTrigger(groupname); err != nil {
		d.logger.Error("Ensure Trigger", err)
		return
	}

	users, err := d.ListNonSuperUsers()
	if err != nil {
		return
	}

	for _, user := range users {
		grantPrivilegesStatement := fmt.Sprintf(`grant "%s" to "%s"`, groupname, user)
		d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

		if _, err := d.DB.Exec(grantPrivilegesStatement); err != nil {
			d.logger.Error("sql-error", err)
			continue
		}

		reassignStatement := fmt.Sprintf(`reassign owned by "%s" to "%s"`, user, groupname)
		d.logger.Debug("reassign-objects", lager.Data{"statement": reassignStatement})

		if _, err := d.DB.Exec(reassignStatement); err != nil {
			d.logger.Error("sql-error", err)
			continue
		}

		d.logger.Info(fmt.Sprintf("Completed migration of %s to use event triggers", dbname))
	}
}
