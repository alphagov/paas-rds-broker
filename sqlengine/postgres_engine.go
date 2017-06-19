package sqlengine

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/lib/pq" // PostgreSQL Driver

	"code.cloudfoundry.org/lager"
)

var ensureTriggerPattern = `
create or replace function reassign_owned() returns event_trigger language plpgsql as $$
begin
	IF pg_has_role(current_user, '{{.role}}', 'member') AND
	   NOT EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true)
	THEN
		EXECUTE 'reassign owned by "' || current_user || '" to "{{.role}}"';
	end if;
end
$$;
`
var ensureGroupPattern = `
do
$body$
begin
	IF NOT EXISTS (select 1 from pg_catalog.pg_roles where rolname = '{{.role}}') THEN
		CREATE ROLE "{{.role}}";
	END IF;
end
$body$
`

const ensureCreateUserPattern = `
DO
$body$
BEGIN
   IF NOT EXISTS (
      SELECT *
      FROM   pg_catalog.pg_user
      WHERE  usename = '{{.user}}') THEN

      CREATE USER {{.user}} WITH PASSWORD '{{.password}}';
   END IF;
END
$body$;`

var ensureTriggerTemplate = template.Must(template.New("ensureTrigger").Parse(ensureTriggerPattern))
var ensureGroupTemplate = template.Must(template.New("ensureGroup").Parse(ensureGroupPattern))
var ensureCreateUserTemplate = template.Must(template.New("ensureUser").Parse(ensureCreateUserPattern))

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

	if err = d.EnsureGroup(dbname, groupname); err != nil {
		return "", "", err
	}

	if err = d.EnsureTrigger(groupname); err != nil {
		return "", "", err
	}

	username = generateUsername(bindingID)
	password = generatePassword()


	if err = d.EnsureUser(dbname, username, password); err != nil {
		return "", "", err
	}

	grantPrivilegesStatement := fmt.Sprintf(`grant "%s" to "%s";`, groupname, username)
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := d.DB.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("Grant sql-error", err)
		return "", "", err
	}

	err = d.MigrateLegacyAdminUsers(bindingID, dbname)
	if err != nil {
		d.logger.Error("Migrate sql-error", err)
	}

	return username, password, nil
}

func (d *PostgresEngine) MigrateLegacyAdminUsers(bindingID, dbname string) (err error) {
	groupname := d.GeneratePostgresGroup(dbname)
	usersMigrate, err := d.ListLegacyAdminUsers()
	if err != nil {
		return err
	}

	for _, user := range usersMigrate {
		grantPrivilegesStatement := fmt.Sprintf(`grant "%s" to "%s";`, groupname, user)
		d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

		if _, err := d.DB.Exec(grantPrivilegesStatement); err != nil {
			d.logger.Error("sql-error", err)
			return err
		}

		reassignStatement := fmt.Sprintf(`reassign owned by "%s" to "%s";`, user, groupname)
		d.logger.Debug("reassign-objects", lager.Data{"statement": reassignStatement})

		if _, err := d.DB.Exec(reassignStatement); err != nil {
			d.logger.Error("sql-error", err)
			return err
		}

	}
	return nil
}

func (d *PostgresEngine) ListLegacyAdminUsers() ([]string, error) {
	usersMigrate := []string{}

	rows, err := d.DB.Query("SELECT usename FROM pg_user WHERE usename LIKE '%owner';")
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
		usersMigrate = append(usersMigrate, username)
	}
	return usersMigrate, nil
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

func (d *PostgresEngine) EnsureUser(dbname string, username string, password string) error {
	var ensureUserStatement bytes.Buffer
	if err := ensureCreateUserTemplate.Execute(&ensureUserStatement, map[string]string{
		"password": password,
		"user": username,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-user", lager.Data{"statement": ensureUserStatement.String()})

	if _, err := d.DB.Exec(ensureUserStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}
