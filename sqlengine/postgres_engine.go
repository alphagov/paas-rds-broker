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
	UsernameGenerator func(seed string) string
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

func (d *PostgresEngine) execCreateUser(tx *sql.Tx, bindingID, dbname, masterUsername string, readOnly *bool) (username, password string, err error) {
	if readOnly != nil {
		d.logger.Debug("exec-create-user",
			lager.Data{
				"BindingID":      bindingID,
				"DatabaseName":   dbname,
				"MasterUsername": masterUsername,
				"READ-ONLY":      *readOnly,
			})
	} else {
		d.logger.Debug("exec-create-user",
			lager.Data{
				"BindingID":      bindingID,
				"DatabaseName":   dbname,
				"MasterUsername": masterUsername,
				"READ-ONLY":      "nil",
			})
	}

	readwriteRoleName, readonlyRoleName := generatePostgresRoleNames(dbname)

	if err = d.ensureRoles(tx, dbname, masterUsername); err != nil {
		return "", "", err
	}

	if err = d.ensureTrigger(tx, readwriteRoleName, readonlyRoleName); err != nil {
		return "", "", err
	}

	username = d.UsernameGenerator(bindingID)
	password = generatePassword()

	if err = d.ensureUser(tx, dbname, username, password); err != nil {
		return "", "", err
	}

	var grantPrivilegesStatement string
	if readOnly != nil && (*readOnly) {
		d.logger.Debug("exec-create-user", lager.Data{"statement": "adding user to a READ-ONLY role"})
		grantPrivilegesStatement = fmt.Sprintf(`grant "%s" to "%s"`, readonlyRoleName, username)
	} else {
		d.logger.Debug("exec-create-user", lager.Data{"statement": "adding user to a READ-WRITE role"})
		grantPrivilegesStatement = fmt.Sprintf(`grant "%s" to "%s";`, readwriteRoleName, username)
	}

	d.logger.Debug("exec-create-user", lager.Data{"statement": grantPrivilegesStatement})

	if _, err := tx.Exec(grantPrivilegesStatement); err != nil {
		d.logger.Error("exec-create-user sql-error", err)
		return "", "", err
	}

	return username, password, nil
}

func (d *PostgresEngine) createUser(bindingID, dbname, masterUsername string, readOnly *bool) (username, password string, err error) {
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}
	username, password, err = d.execCreateUser(tx, bindingID, dbname, masterUsername, readOnly)
	if err != nil {
		_ = tx.Rollback()
		return "", "", err
	}
	return username, password, tx.Commit()
}

func (d *PostgresEngine) CreateUser(bindingID, dbname, masterUsername string, readOnly *bool) (username, password string, err error) {
	var pqErr *pq.Error
	tries := 0
	for tries < 10 {
		tries++
		username, password, err := d.createUser(bindingID, dbname, masterUsername, readOnly)
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

// generatePostgresRoleNames produces a deterministic role names. This is because these roles
// will be persisted across all application bindings.
// first returned string is RW_rolename
// second returned string is RO_rolename
func generatePostgresRoleNames(dbname string) (string, string) {
	return dbname + "_manager", dbname + "_readonly"
}

const ensureRolesPattern = `
	DO
	$body$
	DECLARE
 		r record;
	BEGIN
		IF NOT EXISTS (select 1 from pg_catalog.pg_roles where rolname = '{{.read_only_role}}') THEN
			CREATE ROLE "{{.read_only_role}}";
		END IF;	
		
		IF NOT EXISTS (select 1 from pg_catalog.pg_roles where rolname = '{{.read_write_role}}') THEN
			CREATE ROLE "{{.read_write_role}}";
		END IF;

		-- GRANT the Master User 'readwrite' role, so we can issue 
		-- 'ALTER DEFAULT PRIVILEDGES' and 'GRANTS' to roles
		GRANT "{{.read_write_role}}" TO "{{.master_user}}";

		--REVOKE permissions from PUBLIC role on DB and SCHEMA
		REVOKE ALL ON DATABASE "{{.database}}" FROM PUBLIC;

		-- Start iterating REVOKE/GRANT statements on all the active schemas

		FOR r in (select schema_name from information_schema.schemata where (schema_owner='{{.read_write_role}}' OR schema_owner='{{.master_user}}') AND catalog_name='{{.database}}') LOOP
			EXECUTE format('REVOKE CREATE ON SCHEMA %s FROM PUBLIC', r.schema_name);
			-- Make sure that master_user (the user that is generated with rds_superuser role)
			-- is set with appropriate access after revokation in the previous step
			EXECUTE format('GRANT ALL ON SCHEMA %s TO "{{.master_user}}"', r.schema_name);
			EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA %s TO "{{.master_user}}"', r.schema_name);
			--GRANT usage on schema 'public' to readonly role
			EXECUTE format('GRANT USAGE ON SCHEMA %s TO {{.read_only_role}}', r.schema_name);
			--GRANT SELECT ON ALL TABLES to readonly role
			EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %s TO {{.read_only_role}}', r.schema_name);
			--GRANT SELECT on all FUTURE TABLES for readonly role
			EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO {{.read_only_role}}', r.schema_name);
			--GRANT ALL PRIVEILEGES on SHCEMA, TABLES, ETC to readwrite role.
			EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA %s TO {{.read_write_role}}', r.schema_name);
			-- Granting all privileges on all current tables
			EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO {{.read_write_role}}', r.schema_name);
			-- granting all privileges on all future tables
			EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON TABLES TO {{.read_write_role}}', r.schema_name);
			-- not sure if we need the following line
			EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE {{.read_write_role}} IN SCHEMA %s GRANT ALL ON TABLES TO {{.read_write_role}}', r.schema_name);
			-- Granting privileges to sequences
			EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO {{.read_write_role}}', r.schema_name);
			EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON SEQUENCES TO {{.read_write_role}}', r.schema_name);
			-- not sure if we need the following line
			EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE {{.read_write_role}} IN SCHEMA %s GRANT SELECT ON TABLES TO {{.read_only_role}}', r.schema_name);
		END LOOP;

		-- Make sure that master_user (the user that is generated with rds_superuser role)
		-- is set with appropriate access after revokation in the previous step
		GRANT ALL ON DATABASE "{{.database}}" TO "{{.master_user}}";
		
		-- GRANT Connect on database to readonly role
		GRANT CONNECT ON DATABASE "{{.database}}" TO "{{.read_only_role}}";
	
		--GRANT ALL PRIVEILEGES on DB readwrite role.
		GRANT ALL PRIVILEGES ON DATABASE "{{.database}}" TO "{{.read_write_role}}";
	END
	$body$
	`

var ensureRolesTemplate = template.Must(template.New("ensureRoles").Parse(ensureRolesPattern))

func (d *PostgresEngine) ensureRoles(tx *sql.Tx, dbname string, masterUsername string) error {
	readwriteRoleName, readonlyRoleName := generatePostgresRoleNames(dbname)
	var ensureRolesStatement bytes.Buffer
	if err := ensureRolesTemplate.Execute(&ensureRolesStatement, map[string]string{
		"read_only_role":  readonlyRoleName,
		"read_write_role": readwriteRoleName,
		"database":        dbname,
		"master_user":     masterUsername,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-roles", lager.Data{"statement": ensureRolesStatement.String()})

	if _, err := tx.Exec(ensureRolesStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}
	return nil
}

const triggerCreateSetOwner = "reassign_owned"

const ensureTriggerPattern = `
--
-- This function is changing the owner of the table to the role called 'readwrite', so each 
-- user of this role will be able to access the tables created by other users from the same group.
-- In addition, it grants select on the created table to 'readonly' role.
--
CREATE OR REPLACE FUNCTION reassign_owned()
  RETURNS event_trigger
  LANGUAGE plpgsql
AS $$
DECLARE r RECORD;
BEGIN
 		-- do not execute if member of rds_superuser
		IF EXISTS (select 1 from pg_catalog.pg_roles where rolname = 'rds_superuser')
		AND pg_has_role(current_user, 'rds_superuser', 'member') THEN
			RETURN;
		END IF;
		-- do not execute if not member of readwrite role
		IF NOT pg_has_role(current_user, '{{.read_write_role}}', 'member') THEN
			RETURN;
		END IF;
		-- do not execute if superuser
		IF EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
			RETURN;
		END IF;

    -- extract the details of the ddl command that triggered this function from pg_event_trigger_ddl_commands
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
      IF r.command_tag LIKE 'CREATE %' THEN
				-- Since this 'raise notice' command is very useful in debugging, 
				-- I would keep it here, but will comment it out for the relase.
				-- RAISE NOTICE 'tg_tag %, tg_event %, command_tag %, object_type %, object_identity %, schema_name %', tg_tag, tg_event, r.command_tag, r.object_type, r.object_identity, r.schema_name;
				
				-- Change the owner of the created object to readwrite role
				EXECUTE 'reassign owned by current_user to {{.read_write_role}}';

				--Grant priveleges to readonly role on a newly created object
				IF r.command_tag = 'CREATE TABLE' OR r.command_tag = 'CREATE TABLE AS' THEN
					-- Grant SELECT priviledge to readonly role on the newly created table
					EXECUTE format('GRANT SELECT ON TABLE %s TO {{.read_only_role}}', r.object_identity);
				ELSIF r.command_tag = 'CREATE SEQUENCE' THEN
					-- Grant USAGE and SELECT priviledge to readonly role on a newly created sequence
					EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE %s TO {{.read_only_role}}', r.object_identity);
				ELSIF r.command_tag = 'CREATE FUNCTION' THEN
					-- Grant EXECUTE priviledge to readonly role on a newly created function
					EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO {{.read_only_role}}', r.object_identity);
				ELSIF r.command_tag = 'CREATE SCHEMA' THEN
					-- Grant USAGE to readonly role on a newly created SCHEMA
					EXECUTE format('GRANT USAGE ON SCHEMA %s TO {{.read_only_role}}', r.object_identity);
				ELSIF r.command_tag = 'CREATE TYPE' THEN
					-- Grant USAGE to readonly role on a newly created TYPE
					EXECUTE format('GRANT USAGE ON TYPE %s TO {{.read_only_role}}', r.object_identity);
				END IF;
			END IF;
    END LOOP;
END;
$$;
`

var ensureTriggerTemplate = template.Must(template.New("ensureTrigger").Parse(ensureTriggerPattern))

func (d *PostgresEngine) ensureTrigger(tx *sql.Tx, readwriteRoleName string, readonlyRoleName string) error {
	var ensureTriggerStatement bytes.Buffer

	if err := ensureTriggerTemplate.Execute(&ensureTriggerStatement, map[string]string{
		"read_write_role": readwriteRoleName,
		"read_only_role":  readonlyRoleName,
	}); err != nil {
		return err
	}

	cmds := []string{
		ensureTriggerStatement.String(),
		fmt.Sprintf(`DROP EVENT TRIGGER IF EXISTS %s;`, triggerCreateSetOwner),
		fmt.Sprintf(`CREATE EVENT TRIGGER %s ON ddl_command_end EXECUTE PROCEDURE %s();`, triggerCreateSetOwner, triggerCreateSetOwner),
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
	d.logger.Debug("ensure-user-with-password", lager.Data{"statement": ensureUserStatement.String()})

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
