package sqlengine

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"text/template"
	"time"

	"github.com/lib/pq" // PostgreSQL Driver

	"code.cloudfoundry.org/lager"
)

const (
	pqErrUniqueViolation  = "23505"
	pqErrDuplicateContent = "42710"
	pqErrInternalError    = "XX000"
	pqErrInvalidPassword  = "28P01"
)

type PostgresEngine struct {
	logger            lager.Logger
	db                *sql.DB
	requireSSL        bool
	UsernameGenerator func(string) string
}

func NewPostgresEngine(logger lager.Logger) *PostgresEngine {
	return &PostgresEngine{
		logger:            logger.Session("postgres-engine"),
		requireSSL:        true,
		UsernameGenerator: generateUsername,
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
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == pqErrInvalidPassword {
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

func (d *PostgresEngine) execCreateUser(tx *sql.Tx, bindingID, dbname string) (username, password string, err error) {
	groupname := d.generatePostgresGroup(dbname)

	if err = d.ensureGroup(tx, dbname, groupname); err != nil {
		return "", "", err
	}

	if err = d.ensureTrigger(tx, groupname); err != nil {
		return "", "", err
	}

	username = d.UsernameGenerator(bindingID)
	password = generatePassword()

	if err = d.ensureUser(tx, dbname, username, password); err != nil {
		return "", "", err
	}

	grantPrivilegesStatement := fmt.Sprintf(`grant "%s" to "%s"`, groupname, username)
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := tx.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("Grant sql-error", err)
		return "", "", err
	}

	grantAllOnDatabaseStatement := fmt.Sprintf(`grant all privileges on database "%s" to "%s"`, dbname, groupname)
	d.logger.Debug("grant-privileges", lager.Data{"statement": grantAllOnDatabaseStatement})

	if _, err := tx.Exec(grantAllOnDatabaseStatement); err != nil {
		d.logger.Error("Grant sql-error", err)
		return "", "", err
	}

	return username, password, nil
}

func (d *PostgresEngine) createUser(bindingID, dbname string) (username, password string, err error) {
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

func (d *PostgresEngine) CreateUser(bindingID, dbname string) (username, password string, err error) {
	var pqErr *pq.Error
	tries := 0
	for tries < 10 {
		tries++
		username, password, err := d.createUser(bindingID, dbname)
		if err != nil {
			var ok bool
			pqErr, ok = err.(*pq.Error)
			if ok && (pqErr.Code == pqErrInternalError || pqErr.Code == pqErrDuplicateContent || pqErr.Code == pqErrUniqueViolation) {
				time.Sleep(time.Duration(rand.Intn(1500)) * time.Millisecond)
				continue
			}
			return "", "", err
		}
		return username, password, nil
	}
	return "", "", pqErr

}

func (d *PostgresEngine) DropUser(bindingID string) error {
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

func (d *PostgresEngine) ResetState() error {
	d.logger.Debug("reset-state.start")

	tx, err := d.db.Begin()
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

	users, err := d.listNonSuperUsers()
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

func (d *PostgresEngine) listNonSuperUsers() ([]string, error) {
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

func (d *PostgresEngine) URI(address string, port int64, dbname string, username string, password string) string {
	uri := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", username, password, address, port, dbname)
	if !d.requireSSL {
		uri = uri + "?sslmode=disable"
	}
	return uri
}

func (d *PostgresEngine) JDBCURI(address string, port int64, dbname string, username string, password string) string {
	params := &url.Values{}
	params.Set("user", username)
	params.Set("password", password)

	if d.requireSSL {
		params.Set("ssl", "true")
	}
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?%s", address, port, dbname, params.Encode())
}

const createExtensionPattern = `CREATE EXTENSION IF NOT EXISTS "{{.extension}}"`
const dropExtensionPattern = `DROP EXTENSION IF EXISTS "{{.extension}}"`

func (d *PostgresEngine) CreateExtensions(extensions []string) error {
	for _, extension := range extensions {
		createExtensionTemplate := template.Must(template.New(extension + "Extension").Parse(createExtensionPattern))
		var createExtensionStatement bytes.Buffer
		if err := createExtensionTemplate.Execute(&createExtensionStatement, map[string]string{"extension": extension}); err != nil {
			return err
		}
		if _, err := d.db.Exec(createExtensionStatement.String()); err != nil {
			return err
		}
	}
	return nil
}

func (d *PostgresEngine) DropExtensions(extensions []string) error {
	for _, extension := range extensions {
		dropExtensionTemplate := template.Must(template.New(extension + "Extension").Parse(dropExtensionPattern))
		var dropExtensionStatement bytes.Buffer
		if err := dropExtensionTemplate.Execute(&dropExtensionStatement, map[string]string{"extension": extension}); err != nil {
			return err
		}
		if _, err := d.db.Exec(dropExtensionStatement.String()); err != nil {
			return err
		}
	}
	return nil
}

// generatePostgresGroup produces a deterministic group name. This is because the role
// will be persisted across all application bindings
func (d *PostgresEngine) generatePostgresGroup(dbname string) string {
	return dbname + "_manager"
}

const ensureGroupPattern = `
	do
	$body$
	begin
		IF NOT EXISTS (select 1 from pg_catalog.pg_roles where rolname = '{{.role}}') THEN
			CREATE ROLE "{{.role}}";
		END IF;
	end
	$body$
	`

var ensureGroupTemplate = template.Must(template.New("ensureGroup").Parse(ensureGroupPattern))

func (d *PostgresEngine) ensureGroup(tx *sql.Tx, dbname, groupname string) error {
	var ensureGroupStatement bytes.Buffer
	if err := ensureGroupTemplate.Execute(&ensureGroupStatement, map[string]string{
		"role": groupname,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-group", lager.Data{"statement": ensureGroupStatement.String()})

	if _, err := tx.Exec(ensureGroupStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

const ensureTriggerPattern = `
	create or replace function reassign_owned() returns event_trigger language plpgsql as $$
	begin
		-- do not execute if member of rds_superuser
		IF EXISTS (select 1 from pg_catalog.pg_roles where rolname = 'rds_superuser')
		AND pg_has_role(current_user, 'rds_superuser', 'member') THEN
			RETURN;
		END IF;

		-- do not execute if not member of manager role
		IF NOT pg_has_role(current_user, '{{.role}}', 'member') THEN
			RETURN;
		END IF;

		-- do not execute if superuser
		IF EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
			RETURN;
		END IF;

		EXECUTE 'reassign owned by "' || current_user || '" to "{{.role}}"';
	end
	$$;
	`

var ensureTriggerTemplate = template.Must(template.New("ensureTrigger").Parse(ensureTriggerPattern))

func (d *PostgresEngine) ensureTrigger(tx *sql.Tx, groupname string) error {
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
		_, err := tx.Exec(cmd)
		if err != nil {
			d.logger.Error("sql-error", err)
			return err
		}
	}

	return nil
}

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

var ensureCreateUserTemplate = template.Must(template.New("ensureUser").Parse(ensureCreateUserPattern))

func (d *PostgresEngine) ensureUser(tx *sql.Tx, dbname string, username string, password string) error {
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

func (d *PostgresEngine) CreateSchema(schemaname string) error {
	const createSchemaStatement = `CREATE SCHEMA IF NOT EXISTS {{.name}};`
	var createSchemaTemplate = template.Must(template.New("createSchemaTemplate").Parse(createSchemaStatement))

	var ensureStatement bytes.Buffer
	err := createSchemaTemplate.Execute(&ensureStatement, map[string]string{"name": schemaname});
	if err != nil {
		d.logger.Error("validation-error", err)
		return err
	}
	d.logger.Debug("ensure-create-schema", lager.Data{"statement": ensureStatement.String()})

	// todo: wrap in retries
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	d.logger.Debug("create-schema-statement", lager.Data{"statement": ensureStatement})

	_, err = tx.Exec(ensureStatement.String())
	if err != nil {
		d.logger.Error("create-schema-sql-error", err)
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (d *PostgresEngine) DropSchema(schemaname string) error {
	const dropSchemaStatement = `DROP SCHEMA IF EXISTS {{.name}};`
	var dropSchemaTemplate = template.Must(template.New("dropSchemaTemplate").Parse(dropSchemaStatement))

	var ensureStatement bytes.Buffer
	err := dropSchemaTemplate.Execute(&ensureStatement, map[string]string{"name": schemaname});
	if err != nil {
		d.logger.Error("validation-error", err)
		return err
	}
	d.logger.Debug("ensure-drop-schema", lager.Data{"statement": ensureStatement.String()})

	// todo: wrap in retries
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	d.logger.Debug("drop-schema-statement", lager.Data{"statement": ensureStatement})

	_, err = tx.Exec(ensureStatement.String())
	if err != nil {
		d.logger.Error("drop-schema-sql-error", err)
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (d *PostgresEngine) GrantPrivileges(
	alterPrivileges bool,
	schemaName string,
	grantType string,
	grantOn string,
	roleName string) error {
	var ensureStatement bytes.Buffer

	if !alterPrivileges {
		const grantStatement = `GRANT {{.grantType}} ON {{.schemaName}} TO {{.roleName}};`
		var grantTemplate = template.Must(template.New("grantTemplate").Parse(grantStatement))
		err := grantTemplate.Execute(&ensureStatement, map[string]string{"grantType": grantType, "schemaName": schemaName, "roleName": roleName})
		if err != nil {
			d.logger.Error("validation-error", err)
			return err
		}
  } else {
		const alterStatement = `ALTER DEFAULT PRIVILEGES IN SCHEMA {{.schemaName}} GRANT {{.grantType}} ON {{.grantOn}} TO {{.roleName}};`
		var alterTemplate = template.Must(template.New("alterTemplate").Parse(alterStatement))
		err := alterTemplate.Execute(&ensureStatement, map[string]string{"schemaName": schemaName, "grantType": grantType, "grantOn": grantOn, "roleName": roleName})
		if err != nil {
			d.logger.Error("validation-error", err)
			return err
		}
	}

	d.logger.Debug("grant-privileges-statement", lager.Data{"statement": ensureStatement.String()})


	return execStatement(d, ensureStatement.String())
}

func (d *PostgresEngine) RevokePrivileges(
	alterPrivileges bool,
	schemaName string,
	grantType string,
	grantOn string,
	roleName string) error {
	return nil
}

func (d *PostgresEngine) ExecuteStatement(statement string) error {
	// todo: sanitize / validate
	return execStatement(d, statement)
}

func execStatement(d *PostgresEngine, statement string) error {
	// todo: wrap in retries
	d.logger.Debug("begin-transaction")
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-db-error", err)
		return err
	}

	d.logger.Debug("exec-transaction")
	_, err = tx.Exec(statement)
	if err != nil {
		d.logger.Error("sql-exec-error", err)
		_ = tx.Rollback()
		return err
	}

	d.logger.Debug("end-transaction")
	return tx.Commit()
}
