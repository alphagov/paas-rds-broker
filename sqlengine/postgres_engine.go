package sqlengine

import (
	"bytes"
	"database/sql"
	"encoding/json"
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

func (d *PostgresEngine) execCreateUser(tx *sql.Tx, bindingID, dbname string, userBindParameters PostgresUserBindParameters) (username, password string, err error) {
	groupname := d.generatePostgresGroup(dbname)

	if err = d.ensureGroup(tx, dbname, groupname); err != nil {
		return "", "", err
	}

	if err = d.ensureTrigger(tx, groupname); err != nil {
		return "", "", err
	}

	if err = d.ensurePublicPrivileges(tx, dbname); err != nil {
		return "", "", err
	}

	username = d.UsernameGenerator(bindingID)
	password = generatePassword()

	if err = d.ensureUser(tx, dbname, username, password); err != nil {
		return "", "", err
	}

	if userBindParameters.IsOwner == nil || *userBindParameters.IsOwner {
		grantMembershipStatement := fmt.Sprintf(`grant "%s" to "%s"`, groupname, username)
		d.logger.Debug("grant-group-membership", lager.Data{"statement": grantMembershipStatement})

		if _, err := tx.Exec(grantMembershipStatement); err != nil {
			d.logger.Error("Grant sql-error", err)
			return "", "", err
		}
	} else {
		defaultPrivilegePlPgSQL := userBindParameters.GetDefaultPrivilegePlPgSQL(username, dbname)

		if defaultPrivilegePlPgSQL != "" {
			d.logger.Debug("set-nonowner-default-privileges", lager.Data{"statement": defaultPrivilegePlPgSQL})

			if _, err := tx.Exec(fmt.Sprintf("DO %s", pq.QuoteLiteral(defaultPrivilegePlPgSQL))); err != nil {
				d.logger.Error("Grant sql-error", err)
				return "", "", err
			}
		}

		privilegeAssignmentPlPgSQL := userBindParameters.GetPrivilegeAssignmentPlPgSQL(username, dbname)

		if privilegeAssignmentPlPgSQL != "" {
			d.logger.Debug("set-nonowner-privilege-assignment", lager.Data{"statement": privilegeAssignmentPlPgSQL})

			if _, err := tx.Exec(fmt.Sprintf("DO %s", pq.QuoteLiteral(privilegeAssignmentPlPgSQL))); err != nil {
				d.logger.Error("Grant sql-error", err)
				return "", "", err
			}
		}

		if err = d.ensureNonOwnerRestrictions(tx, username, dbname); err != nil {
			return "", "", err
		}
	}

	if err = d.ensureGroupPrivileges(tx, dbname, groupname); err != nil {
		return "", "", err
	}

	return username, password, nil
}

func (d *PostgresEngine) createUser(bindingID, dbname string, userBindParameters PostgresUserBindParameters) (username, password string, err error) {
	tx, err := d.db.Begin()
	if err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}
	username, password, err = d.execCreateUser(tx, bindingID, dbname, userBindParameters)
	if err != nil {
		_ = tx.Rollback()
		return "", "", err
	}
	return username, password, tx.Commit()
}

func (d *PostgresEngine) CreateUser(bindingID, dbname string, userBindParametersRaw *json.RawMessage) (username, password string, err error) {
	var pgServerVersionNum int
	err = d.db.QueryRow("SELECT current_setting('server_version_num')::integer").Scan(&pgServerVersionNum)
	if err != nil {
		d.logger.Error("sql-error", err)
		return "", "", err
	}

	bindParameters := PostgresUserBindParameters{}
	if userBindParametersRaw != nil && len(*userBindParametersRaw) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(*userBindParametersRaw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&bindParameters); err != nil {
			return "", "", err
		}
		if err := bindParameters.Validate(pgServerVersionNum); err != nil {
			return "", "", err
		}
	}

	var pqErr *pq.Error
	tries := 0
	for tries < 10 {
		tries++
		username, password, err := d.createUser(bindingID, dbname, bindParameters)
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

const excIgnoreWrapper = `DO $$
BEGIN
	DECLARE
		message text;
	BEGIN
		%s;
	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS message = MESSAGE_TEXT;
			RAISE WARNING 'swallowed ERROR: %%', message;
	END;
END;
$$`

func (d *PostgresEngine) dropUser(tx *sql.Tx, username, dbname string) (success bool, err error) {
	groupname := d.generatePostgresGroup(dbname)

	statement := "SELECT EXISTS(SELECT 1 FROM pg_user WHERE usename = $1)"
	d.logger.Debug("role-check", lager.Data{"statement": statement, "username": username})
	var found bool
	err = tx.QueryRow(statement, username).Scan(&found)
	if err != nil {
		d.logger.Error("sql-error", err)
		return false, err
	}
	if found {
		// in case the user managed to create an object that didn't get reassigned to the master group, attempt
		// a reassignment now.
		// however we wrap this in an exception-swallowing plpgsql block as there hopefully shouldn't be any such
		// owned objects anyway and we want to be able to continue with an unspoiled transaction
		statement = fmt.Sprintf(excIgnoreWrapper, fmt.Sprintf(`REASSIGN OWNED BY %s TO %s`, pq.QuoteIdentifier(username), pq.QuoteIdentifier(groupname)))
		d.logger.Debug("reassign-owned", lager.Data{"statement": statement})
		if _, err = tx.Exec(statement); err != nil {
			d.logger.Error("sql-error", err)
			return false, err
		}
		// the only things left should be privileges, which should be safely droppable, however use RESTRICT to
		// guard against any possibility of data loss.
		// however we wrap this too in an exception-swallowing plpgsql block as there might not be any such owned
		// objects anyway and we want to be able to continue with an unspoiled transaction
		statement = fmt.Sprintf(excIgnoreWrapper, fmt.Sprintf(`DROP OWNED BY %s RESTRICT`, pq.QuoteIdentifier(username)))
		d.logger.Debug("drop-owned", lager.Data{"statement": statement})
		if _, err = tx.Exec(statement); err != nil {
			d.logger.Error("sql-error", err)
			return false, err
		}

		statement = fmt.Sprintf(`DROP ROLE %s`, pq.QuoteIdentifier(username))
		d.logger.Debug("drop-role", lager.Data{"statement": statement})
		if _, err = tx.Exec(statement); err != nil {
			d.logger.Error("sql-error", err)
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (d *PostgresEngine) DropUser(bindingID, dbname string) error {
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

	username := d.UsernameGenerator(bindingID)

	success, err := d.dropUser(tx, username, dbname)
	if err != nil {
		return err
	}
	if !success {
		d.logger.Info("warning", lager.Data{"warning": "User " + username + " does not exist"})

		// When handling unbinds for bindings created before the switch to
		// event-triggers based permissions the `username` won't exist.
		// Also we changed how we generate usernames so we have to try to drop the username generated
		// the old way.
		username = generateUsernameOld(bindingID)
		success, err = d.dropUser(tx, username, dbname)
		if err != nil {
			return err
		}
		if !success {
			// If none of the usernames exist then we swallow the error
			d.logger.Info("warning", lager.Data{"warning": "User " + username + " does not exist"})
			return nil
		}
	}

	// all unsuccessful cases have returned by now
	err = tx.Commit()
	if err != nil {
		d.logger.Error("commit.sql-error", err)
		return err
	}
	commitCalled = true // Prevent Rollback being called in deferred function
	return nil
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
		dropOwnedStatement := fmt.Sprintf(`drop owned by "%s"`, username)
		d.logger.Debug("reset-state", lager.Data{"statement": dropOwnedStatement})
		if _, err = tx.Exec(dropOwnedStatement); err != nil {
			d.logger.Error("sql-error", err)
			return err
		}

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

const ensurePublicPrivilegesPattern = `
	do
	$body$
	declare
		r record;
	begin
		-- these privileges must be revoked from PUBLIC else we will not be able to restrict
		-- access to them in a more fine-grained way later
		ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM PUBLIC;
		ALTER DEFAULT PRIVILEGES REVOKE ALL ON SEQUENCES FROM PUBLIC;
		IF current_setting('server_version_num')::integer >= 100000 THEN
			-- only supported from pg 10 onwards, we must emulate support in our trigger otherwise.
			-- we also have to disguise it in an EXECUTE to prevent it upsetting the plpgsql parser
			-- on older servers
			EXECUTE 'ALTER DEFAULT PRIVILEGES REVOKE ALL ON SCHEMAS FROM PUBLIC';
		END IF;

		REVOKE ALL ON DATABASE "{{.dbname}}" FROM PUBLIC;

		-- there are other privileges for which we don't currently implement a way to further
		-- control, so we have to ensure PUBLIC has access to them to prevent removing abilities
		-- entirely for some users
		ALTER DEFAULT PRIVILEGES GRANT ALL ON FUNCTIONS TO PUBLIC;
		ALTER DEFAULT PRIVILEGES GRANT ALL ON TYPES TO PUBLIC;

		-- now we perform equivalent operations for any objects that might already exist

		FOR r IN SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema' AND schema_name NOT LIKE 'pg_%' LOOP
			-- revoke privileges we can handle more specifically
			EXECUTE 'REVOKE ALL ON ALL TABLES IN SCHEMA ' || quote_ident(r.schema_name) || ' FROM PUBLIC';
			EXECUTE 'REVOKE ALL ON ALL SEQUENCES IN SCHEMA ' || quote_ident(r.schema_name) || ' FROM PUBLIC';
			EXECUTE 'REVOKE ALL ON SCHEMA ' || quote_ident(r.schema_name) || ' FROM PUBLIC';

			-- grant privileges we can't handle more specifically
			EXECUTE 'GRANT ALL ON ALL FUNCTIONS IN SCHEMA ' || quote_ident(r.schema_name) || ' TO PUBLIC';
		END LOOP;

		FOR r IN SELECT user_defined_type_schema, user_defined_type_name FROM information_schema.user_defined_types LOOP
			EXECUTE 'GRANT ALL ON TYPE ' || quote_ident(r.user_defined_type_schema) || '.' || quote_ident(r.user_defined_type_name) || ' TO  PUBLIC';
		END LOOP;

		FOR r IN SELECT domain_schema, domain_name FROM information_schema.domains LOOP
			EXECUTE 'GRANT ALL ON DOMAIN ' || quote_ident(r.domain_schema) || '.' || quote_ident(r.domain_name) || ' TO  PUBLIC';
		END LOOP;

		FOR r IN SELECT lanname FROM pg_catalog.pg_language WHERE lanpltrusted LOOP
			EXECUTE 'GRANT ALL ON LANGUAGE ' || quote_ident(r.lanname) || ' TO  PUBLIC';
		END LOOP;
	end
	$body$
	`

var ensurePublicPrivilegesTemplate = template.Must(template.New("ensurePublicPrivileges").Parse(ensurePublicPrivilegesPattern))

func (d *PostgresEngine) ensurePublicPrivileges(tx *sql.Tx, dbname string) error {
	var ensurePublicPrivilegesStatement bytes.Buffer
	if err := ensurePublicPrivilegesTemplate.Execute(&ensurePublicPrivilegesStatement, map[string]string{
		"dbname": dbname,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-public-privileges", lager.Data{"statement": ensurePublicPrivilegesStatement.String()})

	if _, err := tx.Exec(ensurePublicPrivilegesStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

const ensureGroupPrivilegesPattern = `
	do
	$body$
	declare
		r record;
	begin
		FOR r IN SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema' AND schema_name NOT LIKE 'pg_%' LOOP
			EXECUTE 'GRANT ALL ON ALL TABLES IN SCHEMA ' || quote_ident(r.schema_name) || ' TO "{{.rolename}}"';
			EXECUTE 'GRANT ALL ON ALL SEQUENCES IN SCHEMA ' || quote_ident(r.schema_name) || ' TO "{{.rolename}}"';
			EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(r.schema_name) || ' TO "{{.rolename}}"';
		END LOOP;

		GRANT ALL ON DATABASE "{{.dbname}}" TO "{{.rolename}}";

		ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO "{{.rolename}}";
		ALTER DEFAULT PRIVILEGES GRANT ALL ON SEQUENCES TO "{{.rolename}}";
		IF current_setting('server_version_num')::integer >= 100000 THEN
			-- only supported from pg 10 onwards, we must emulate support in our trigger otherwise.
			-- we also have to disguise it in an EXECUTE to prevent it upsetting the plpgsql parser
			-- on older servers
			EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS TO "{{.rolename}}"';
		END IF;
	end
	$body$
	`

var ensureGroupPrivilegesTemplate = template.Must(template.New("ensureGroupPrivileges").Parse(ensureGroupPrivilegesPattern))

func (d *PostgresEngine) ensureGroupPrivileges(tx *sql.Tx, dbname, groupname string) error {
	var ensureGroupPrivilegesStatement bytes.Buffer
	if err := ensureGroupPrivilegesTemplate.Execute(&ensureGroupPrivilegesStatement, map[string]string{
		"dbname": dbname,
		"rolename": groupname,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-group-privileges", lager.Data{"statement": ensureGroupPrivilegesStatement.String()})

	if _, err := tx.Exec(ensureGroupPrivilegesStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

const ensureNonOwnerRestrictionsPattern = `
	do
	$body$
	declare
		r record;
	begin
		-- we cannot allow any form of CREATE permission for non-owner users as it would cause ownership complications
		FOR r IN SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema' AND schema_name NOT LIKE 'pg_%' LOOP
			EXECUTE 'REVOKE CREATE ON SCHEMA ' || quote_ident(r.schema_name) || ' FROM "{{.rolename}}"';
		END LOOP;

		REVOKE CREATE ON DATABASE "{{.dbname}}" FROM "{{.rolename}}";
	end
	$body$
	`

var ensureNonOwnerRestrictionsTemplate = template.Must(template.New("ensureNonOwnerRestrictions").Parse(ensureNonOwnerRestrictionsPattern))

func (d *PostgresEngine) ensureNonOwnerRestrictions(tx *sql.Tx, username, dbname string) error {
	var ensureNonOwnerRestrictionsStatement bytes.Buffer
	if err := ensureNonOwnerRestrictionsTemplate.Execute(&ensureNonOwnerRestrictionsStatement, map[string]string{
		"rolename": username,
		"dbname": dbname,
	}); err != nil {
		return err
	}
	d.logger.Debug("ensure-non-owner-restrictions", lager.Data{"statement": ensureNonOwnerRestrictionsStatement.String()})

	if _, err := tx.Exec(ensureNonOwnerRestrictionsStatement.String()); err != nil {
		d.logger.Error("sql-error", err)
		return err
	}

	return nil
}

const ensureTriggerPattern = `
	create or replace function reassign_owned() returns event_trigger language plpgsql as $$
	declare
		r record;
	begin
		-- first a slight detour: pg versions before 10 don't support default privileges for schemas, so
		-- we have to emulate that support here, issuing equivalent commands if we detect CREATE SCHEMA
		IF current_setting('server_version_num')::integer < 100000 THEN
			FOR r IN SELECT object_identity FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE SCHEMA' LOOP
				EXECUTE 'GRANT ALL ON SCHEMA ' || r.object_identity || ' TO "{{.role}}"';
				EXECUTE 'REVOKE ALL ON SCHEMA ' || r.object_identity || ' FROM PUBLIC';
			END LOOP;
		END IF;

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
