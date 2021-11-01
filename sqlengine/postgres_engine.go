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

func (d *PostgresEngine) execCreateUser(tx *sql.Tx, bindingID, dbname string, readOnly bool) (username, password string, err error) {
	if err = d.ensureGroup(tx, dbname); err != nil {
		return "", "", err
	}

	if err = d.ensurePermissionsTriggers(tx, dbname); err != nil {
		return "", "", err
	}

	username = d.UsernameGenerator(bindingID)
	password = generatePassword()

	if err = d.ensureUser(tx, dbname, username, password); err != nil {
		return "", "", err
	}

	if readOnly {
		grantPrivilegesStatement := fmt.Sprintf(
			`grant %s to %s`,
			pq.QuoteIdentifier(dbname + "_reader"),
			pq.QuoteIdentifier(username),
		)
		d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

		if _, err := tx.Exec(grantPrivilegesStatement); err != nil {
			d.logger.Error("Grant sql-error", err)
			return "", "", err
		}

		grantConnectOnDatabaseStatement := fmt.Sprintf(
			`grant connect on database %s to %s`,
			pq.QuoteIdentifier(dbname),
			pq.QuoteIdentifier(dbname + "_reader"),
		)
		d.logger.Debug("grant-connect", lager.Data{"statement": grantConnectOnDatabaseStatement})

		if _, err := tx.Exec(grantConnectOnDatabaseStatement); err != nil {
			d.logger.Error("Grant sql-error", err)
			return "", "", err
		}

		makeReadableStatement := `select make_readable_generic()`
		d.logger.Debug("make-readable", lager.Data{"statement": makeReadableStatement})

		if _, err := tx.Exec(makeReadableStatement); err != nil {
			d.logger.Error("Make readable-error", err)
			return "", "", err
		}
	} else {
		grantPrivilegesStatement := fmt.Sprintf(
			`grant %s to %s`,
			pq.QuoteIdentifier(dbname + "_manager"),
			pq.QuoteIdentifier(username),
		)
		d.logger.Debug("grant-privileges", lager.Data{"statement": grantPrivilegesStatement})

		if _, err := tx.Exec(grantPrivilegesStatement); err != nil {
			d.logger.Error("Grant sql-error", err)
			return "", "", err
		}

		grantAllOnDatabaseStatement := fmt.Sprintf(
			`grant all privileges on database %s to %s`,
			pq.QuoteIdentifier(dbname),
			pq.QuoteIdentifier(dbname + "_manager"),
		)
		d.logger.Debug("grant-privileges", lager.Data{"statement": grantAllOnDatabaseStatement})

		if _, err := tx.Exec(grantAllOnDatabaseStatement); err != nil {
			d.logger.Error("Grant sql-error", err)
			return "", "", err
		}
	}

	return username, password, nil
}

func (d *PostgresEngine) createUser(bindingID, dbname string, readOnly bool) (username, password string, err error) {
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}
	username, password, err = d.execCreateUser(tx, bindingID, dbname, readOnly)
	if err != nil {
		_ = tx.Rollback()
		return "", "", err
	}
	return username, password, tx.Commit()
}

func (d *PostgresEngine) CreateUser(bindingID, dbname string, readOnly bool) (username, password string, err error) {
	var pqErr *pq.Error
	tries := 0
	for tries < 10 {
		tries++
		username, password, err := d.createUser(bindingID, dbname, readOnly)
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
	dropUserStatement := fmt.Sprintf(
		`drop role %s`,
		pq.QuoteIdentifier(username),
	)

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
		dropUserStatement = fmt.Sprintf(
			`drop role %s`,
			pq.QuoteIdentifier(username),
		)
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
		dropUserStatement := fmt.Sprintf(
			`drop role %s`,
			pq.QuoteIdentifier(username),
		)
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

	rows, err := d.db.Query(
		"select usename from pg_user where usesuper != true and usename != current_user",
	)
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

const createExtensionPattern = `CREATE EXTENSION IF NOT EXISTS {{.extensionIden}}`
const dropExtensionPattern = `DROP EXTENSION IF EXISTS {{.extensionIden}}`

func (d *PostgresEngine) CreateExtensions(extensions []string) error {
	for _, extension := range extensions {
		createExtensionTemplate := template.Must(template.New(
			extension + "Extension",
		).Parse(createExtensionPattern))
		var createExtensionStatement bytes.Buffer
		if err := createExtensionTemplate.Execute(&createExtensionStatement, map[string]string{
			"extensionIden": pq.QuoteIdentifier(extension),
		}); err != nil {
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
		dropExtensionTemplate := template.Must(template.New(
			extension + "Extension",
		).Parse(dropExtensionPattern))
		var dropExtensionStatement bytes.Buffer
		if err := dropExtensionTemplate.Execute(&dropExtensionStatement, map[string]string{
			"extensionIden": pq.QuoteIdentifier(extension),
		}); err != nil {
			return err
		}
		if _, err := d.db.Exec(dropExtensionStatement.String()); err != nil {
			return err
		}
	}
	return nil
}

const doWrapperPattern = "DO {{.bodyStr}}"

const ensureGroupBodyPattern = `
	begin
		IF NOT EXISTS (select 1 from pg_catalog.pg_roles where rolname = {{.managerRoleStr}}) THEN
			CREATE ROLE {{.managerRoleIden}};
		END IF;

		IF NOT EXISTS (select 1 from pg_catalog.pg_roles where rolname = {{.readerRoleStr}}) THEN
			CREATE ROLE {{.readerRoleIden}} NOINHERIT;
		END IF;
	end
`

var doWrapperTemplate = template.Must(template.New("doWrapper").Parse(doWrapperPattern))
var ensureGroupBodyTemplate = template.Must(template.New("ensureGroupBody").Parse(ensureGroupBodyPattern))

func (d *PostgresEngine) ensureGroup(tx *sql.Tx, dbname string) error {
	var ensureGroupBody bytes.Buffer
	if err := ensureGroupBodyTemplate.Execute(&ensureGroupBody, map[string]string{
		"managerRoleStr": pq.QuoteLiteral(dbname + "_manager"),
		"managerRoleIden": pq.QuoteIdentifier(dbname + "_manager"),
		"readerRoleStr": pq.QuoteLiteral(dbname + "_reader"),
		"readerRoleIden": pq.QuoteIdentifier(dbname + "_reader"),
	}); err != nil {
		return err
	}

	var ensureGroupStatement bytes.Buffer
	if err := doWrapperTemplate.Execute(&ensureGroupStatement, map[string]string{
		"bodyStr": pq.QuoteLiteral(ensureGroupBody.String()),
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

const reassignOwnedBodyPattern = `
	begin
		-- do not execute if member of rds_superuser
		IF EXISTS (select 1 from pg_catalog.pg_roles where rolname = 'rds_superuser')
		AND pg_has_role(current_user, 'rds_superuser', 'member') THEN
			RETURN;
		END IF;

		-- do not execute if superuser
		IF EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
			RETURN;
		END IF;

		-- do not execute if not member of manager role
		IF NOT pg_has_role(current_user, {{.managerRoleStr}}, 'member') THEN
			RETURN;
		END IF;

		EXECUTE format('REASSIGN OWNED BY %I TO %I', current_user, {{.managerRoleStr}});

		RETURN;
	end
`

const makeReadableGenericBodyPattern = `
	declare
		r record;
	begin
		-- do not execute if member of rds_superuser
		IF EXISTS (select 1 from pg_catalog.pg_roles where rolname = 'rds_superuser')
		AND pg_has_role(current_user, 'rds_superuser', 'member') THEN
			RETURN;
		END IF;

		-- do not execute if superuser
		IF EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
			RETURN;
		END IF;

		-- do not execute if not member of manager role
		IF NOT pg_has_role(current_user, {{.managerRoleStr}}, 'member') THEN
			RETURN;
		END IF;

		FOR r in (select schema_name from information_schema.schemata) LOOP
			BEGIN
				EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', r.schema_name, {{.readerRoleStr}});
				EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I', r.schema_name, {{.readerRoleStr}});
				EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', r.schema_name, {{.readerRoleStr}});

				RAISE NOTICE 'GRANTED READ ONLY IN SCHEMA %s', r.schema_name;
			EXCEPTION WHEN OTHERS THEN
			  -- brrr
			END;
		END LOOP;

		RETURN;
	end
`

const forbidDDLReaderBodyPattern = `
	begin
		-- do not execute if member of rds_superuser
		IF EXISTS (select 1 from pg_catalog.pg_roles where rolname = 'rds_superuser')
		AND pg_has_role(current_user, 'rds_superuser', 'member') THEN
			RETURN;
		END IF;

		-- do not execute if superuser
		IF EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
			RETURN;
		END IF;

		-- do not execute if member of manager role
		IF pg_has_role(current_user, {{.managerRoleStr}}, 'member') THEN
			RETURN;
		END IF;

		IF pg_has_role(current_user, {{.readerRoleStr}}, 'member') THEN
			RAISE EXCEPTION 'executing % is disabled for read only bindings', tg_tag;
		END IF;
	end
`

const ensurePermissionsTriggersPattern = `
	create or replace function reassign_owned() returns event_trigger language plpgsql set search_path to public as {{.reassignOwnedBodyStr}};
	create or replace function make_readable_generic() returns void language plpgsql set search_path to public as {{.makeReadableGenericBodyStr}};
	create or replace function make_readable() returns event_trigger language plpgsql set search_path to public as $$
	begin
		EXECUTE 'select make_readable_generic()';
		RETURN;
	end
	$$;
	create or replace function forbid_ddl_reader() returns event_trigger language plpgsql set search_path to public as {{.forbidDDLReaderBodyStr}};
	`

var reassignOwnedBodyTemplate = template.Must(template.New("reassignOwnedBody").Parse(reassignOwnedBodyPattern))
var makeReadableGenericBodyTemplate = template.Must(template.New("makeReadableGenericBody").Parse(makeReadableGenericBodyPattern))
var forbidDDLReaderBodyTemplate = template.Must(template.New("forbidDDLReaderBody").Parse(forbidDDLReaderBodyPattern))
var ensurePermissionsTriggersTemplate = template.Must(template.New("ensurePermissionsTriggers").Parse(ensurePermissionsTriggersPattern))

func (d *PostgresEngine) ensurePermissionsTriggers(tx *sql.Tx, dbname string) error {
	var reassignOwnedBody bytes.Buffer
	if err := reassignOwnedBodyTemplate.Execute(&reassignOwnedBody, map[string]string{
		"managerRoleStr": pq.QuoteLiteral(dbname + "_manager"),
		"readerRoleStr": pq.QuoteLiteral(dbname + "_reader"),
	}); err != nil {
		return err
	}

	var makeReadableGenericBody bytes.Buffer
	if err := makeReadableGenericBodyTemplate.Execute(&makeReadableGenericBody, map[string]string{
		"managerRoleStr": pq.QuoteLiteral(dbname + "_manager"),
		"readerRoleStr": pq.QuoteLiteral(dbname + "_reader"),
	}); err != nil {
		return err
	}

	var forbidDDLReaderBody bytes.Buffer
	if err := forbidDDLReaderBodyTemplate.Execute(&forbidDDLReaderBody, map[string]string{
		"managerRoleStr": pq.QuoteLiteral(dbname + "_manager"),
		"readerRoleStr": pq.QuoteLiteral(dbname + "_reader"),
	}); err != nil {
		return err
	}

	var ensurePermissionsTriggersStatement bytes.Buffer
	if err := ensurePermissionsTriggersTemplate.Execute(&ensurePermissionsTriggersStatement, map[string]string{
		"reassignOwnedBodyStr": pq.QuoteLiteral(reassignOwnedBody.String()),
		"makeReadableGenericBodyStr": pq.QuoteLiteral(makeReadableGenericBody.String()),
		"forbidDDLReaderBodyStr": pq.QuoteLiteral(forbidDDLReaderBody.String()),
	}); err != nil {
		return err
	}

	cmds := []string{
		ensurePermissionsTriggersStatement.String(),
		`drop event trigger if exists reassign_owned;`,
		`create event trigger reassign_owned on ddl_command_end execute procedure reassign_owned();`,
		`drop event trigger if exists make_readable;`,
		`create event trigger make_readable on ddl_command_end when tag in ('CREATE TABLE', 'CREATE TABLE AS', 'CREATE SCHEMA', 'CREATE VIEW', 'CREATE SEQUENCE') execute procedure make_readable();`,
		`drop event trigger if exists forbid_ddl_reader;`,
		`create event trigger forbid_ddl_reader on ddl_command_start execute procedure forbid_ddl_reader();`,
	}

	for _, cmd := range cmds {
		d.logger.Debug("ensure-permissions-triggers", lager.Data{"statement": cmd})
		_, err := tx.Exec(cmd)
		if err != nil {
			d.logger.Error("sql-error", err)
			return err
		}
	}

	return nil
}

const ensureCreateUserBodyPattern = `
	BEGIN
	   IF NOT EXISTS (
		  SELECT *
		  FROM   pg_catalog.pg_user
		  WHERE  usename = {{.userStr}}) THEN

		  CREATE USER {{.userIden}} WITH PASSWORD {{.passwordStr}};
	   END IF;
	END
`

var ensureCreateUserBodyTemplate = template.Must(template.New("ensureUserBody").Parse(ensureCreateUserBodyPattern))

func (d *PostgresEngine) ensureUser(tx *sql.Tx, dbname string, username string, password string) error {
	var ensureCreateUserBody bytes.Buffer
	if err := ensureCreateUserBodyTemplate.Execute(&ensureCreateUserBody, map[string]string{
		"passwordStr": pq.QuoteLiteral(password),
		"userStr": pq.QuoteLiteral(username),
		"userIden": pq.QuoteIdentifier(username),
	}); err != nil {
		return err
	}
	var ensureCreateUserStatement bytes.Buffer
	if err := doWrapperTemplate.Execute(&ensureCreateUserStatement, map[string]string{
		"bodyStr": pq.QuoteLiteral(ensureCreateUserBody.String()),
	}); err != nil {
		return err
	}

	var ensureCreateUserBodySanitized bytes.Buffer
	if err := ensureCreateUserBodyTemplate.Execute(&ensureCreateUserBodySanitized, map[string]string{
		"passwordStr": "REDACTED",
		"userStr": pq.QuoteLiteral(username),
		"userIden": pq.QuoteIdentifier(username),
	}); err != nil {
		return err
	}
	var ensureCreateUserStatementSanitized bytes.Buffer
	if err := doWrapperTemplate.Execute(&ensureCreateUserStatementSanitized, map[string]string{
		"bodyStr": pq.QuoteLiteral(ensureCreateUserBodySanitized.String()),
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-user", lager.Data{"statement": ensureCreateUserStatementSanitized.String()})

	if _, err := tx.Exec(ensureCreateUserStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}
