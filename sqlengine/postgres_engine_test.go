package sqlengine

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/alphagov/paas-rds-broker/utils"
	"github.com/lib/pq"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/lager"
)

func createMasterUser(connectionString string) (string, string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	randomMasterUser := "master_" + utils.RandomLowerAlphaNum(6)
	password := "mypass"

	statement := "CREATE USER " + randomMasterUser + " PASSWORD '" + password + "'"
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())

	statement = "ALTER USER " + randomMasterUser + " WITH SUPERUSER"
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())

	return randomMasterUser, password
}

func dropTestUser(connectionString, username string) {
	// The master connection should be used here. See:
	// https://www.postgresql.org/message-id/83894A1821034948BA27FE4DAA47427928F7C29922%40apde03.APD.Satcom.Local
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	statement := "DROP OWNED BY " + username + " CASCADE"
	_, err = db.Exec(statement)
	if err != nil {
		fmt.Fprintln(GinkgoWriter, err)
	}
	statement = "DROP USER " + username
	_, err = db.Exec(statement)
	if err != nil {
		fmt.Fprintln(GinkgoWriter, err)
	}
}

func createDB(connectionString, dbName string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	statement := "CREATE DATABASE " + dbName
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())
}

func dropDB(connectionString, dbName string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	statement := "DROP DATABASE " + dbName
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())
}

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func createObjects(connectionString, baseName string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	tableName := baseName + "_t"
	schemaName := baseName + "_sc"

	_, err = db.Exec("CREATE TABLE " + tableName + "(col CHAR(8))")
	Expect(err).ToNot(HaveOccurred())

	_, err = db.Exec("INSERT INTO " + tableName + " (col) VALUES ('value')")
	Expect(err).ToNot(HaveOccurred())

	_, err = db.Exec("CREATE SCHEMA " + schemaName)
	Expect(err).ToNot(HaveOccurred())
}

func accessAndDeleteObjects(connectionString, baseName string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	tableName := baseName + "_t"
	schemaName := baseName + "_sc"
	tableName2 := baseName + "_t2"

	var col string
	err = db.QueryRow("SELECT * FROM " + tableName + " WHERE col = 'value'").Scan(&col)
	Expect(err).ToNot(HaveOccurred())
	Expect(strings.TrimSpace(col)).To(BeEquivalentTo("value"))

	_, err = db.Exec("DROP TABLE " + tableName)
	Expect(err).ToNot(HaveOccurred())

	_, err = db.Exec("CREATE TABLE " + schemaName + "." + tableName2 + "(col CHAR(8))")
	Expect(err).ToNot(HaveOccurred())

	_, err = db.Exec("DROP SCHEMA " + schemaName + " CASCADE")
	Expect(err).ToNot(HaveOccurred())
}

func getPgServerVersionNum(connectionString string) int {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()

	var col int
	err = db.QueryRow("SELECT current_setting('server_version_num')::integer").Scan(&col)
	Expect(err).ToNot(HaveOccurred())

	return col
}

var _ = Describe("PostgresEngine", func() {
	var (
		postgresEngine *PostgresEngine
		logger         lager.Logger

		address        string
		port           int64
		dbname         string
		masterUsername string
		masterPassword string

		randomTestSuffix string

		template1ConnectionString string

		pgServerVersionNum int
	)

	BeforeEach(func() {
		logger = lager.NewLogger("provider_service_test")
		logger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		randomTestSuffix = "_" + utils.RandomLowerAlphaNum(6)

		postgresEngine = NewPostgresEngine(logger)
		postgresEngine.requireSSL = false

		address = getEnvOrDefault("POSTGRESQL_HOSTNAME", "localhost")
		portString := getEnvOrDefault("POSTGRESQL_PORT", "5432")
		p, err := strconv.Atoi(portString)
		Expect(err).ToNot(HaveOccurred())
		port = int64(p)

		dbname = "mydb" + randomTestSuffix

		rootUsername := getEnvOrDefault("POSTGRESQL_USERNAME", "postgres")
		rootPassword := getEnvOrDefault("POSTGRESQL_PASSWORD", "")

		template1ConnectionString = postgresEngine.URI(address, port, "template1", rootUsername, rootPassword)

		masterUsername, masterPassword = createMasterUser(template1ConnectionString)

		// Create the test DB
		createDB(template1ConnectionString, dbname)

		pgServerVersionNum = getPgServerVersionNum(template1ConnectionString)
	})

	AfterEach(func() {
		postgresEngine.Close() // Ensure the DB is closed
		dropDB(template1ConnectionString, dbname)
		dropTestUser(template1ConnectionString, masterUsername)
	})

	Context("can construct JDBC URI", func() {

		It("when SSL is enabled", func() {
			postgresEngine.requireSSL = true
			jdbcuri := postgresEngine.JDBCURI(address, port, dbname, masterUsername, masterPassword)
			Expect(jdbcuri).To(ContainSubstring("ssl=true"))
		})

		It("when SSL is disabled", func() {
			postgresEngine.requireSSL = false
			jdbcuri := postgresEngine.JDBCURI(address, port, dbname, masterUsername, masterPassword)
			Expect(jdbcuri).ToNot(ContainSubstring("ssl=true"))
		})
	})

	It("can connect to the new DB", func() {
		err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
		defer postgresEngine.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns error if engine is the database is not reachable", func() {
		err := postgresEngine.Open("localhost", 1, dbname, masterUsername, masterPassword)
		defer postgresEngine.Close()
		Expect(err).To(HaveOccurred())
	})

	It("returns error LoginFailedError if the credentials are wrong", func() {
		err := postgresEngine.Open(address, port, dbname, masterUsername, "wrong_password")
		defer postgresEngine.Close()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(LoginFailedError))
	})

	Describe("Concurrency", func() {

		It("Should be able to handle rapid parallel CreateUser/DropUser from multiple connections", func() {

			var wg sync.WaitGroup

			for i := 0; i < 25; i++ {
				wg.Add(1)
				go func(bindingID string) {
					defer GinkgoRecover()
					defer wg.Done()
					postgresEngine := NewPostgresEngine(logger)
					postgresEngine.requireSSL = false

					err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
					Expect(err).ToNot(HaveOccurred())
					defer postgresEngine.Close()

					_, _, err = postgresEngine.CreateUser(bindingID, dbname, nil)
					Expect(err).ToNot(HaveOccurred())

					err = postgresEngine.DropUser(bindingID, dbname)
					Expect(err).ToNot(HaveOccurred())
				}(fmt.Sprintf("binding-id-%d", i))
			}

			wg.Wait()

		})

	})

	Describe("CreateUser", func() {
		var (
			bindingID       string
			createdUser     string
			createdPassword string
		)

		BeforeEach(func() {
			bindingID = "binding-id" + randomTestSuffix
			err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("Without userBindParameters supplied", func() {
			BeforeEach(func() {
				var err error
				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_before_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SCHEMA sc_created_before_user")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE FUNCTION f_created_before_user() RETURNS integer AS 'BEGIN\nRETURN 321;\nEND;' LANGUAGE plpgsql")
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname, nil)
				Expect(err).ToNot(HaveOccurred())

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_after_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SCHEMA sc_created_after_user")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE FUNCTION f_created_after_user() RETURNS integer AS 'BEGIN\nRETURN 321;\nEND;' LANGUAGE plpgsql")
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			It("CreateUser() returns valid credentials", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()
				err = db.Ping()
				Expect(err).ToNot(HaveOccurred())
			})

			It("creates a user with the necessary permissions on the database", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TABLE foo (col CHAR(8))")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("INSERT INTO foo (col) VALUES ('value')")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("CREATE SCHEMA bar")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("CREATE TABLE bar.baz (col CHAR(8))")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("INSERT INTO bar.baz (col) VALUES ('other')")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("DROP TABLE bar.baz")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("DROP SCHEMA bar CASCADE")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT * FROM t_created_before_user")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT * FROM t_created_after_user")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("CREATE TABLE sc_created_before_user.qux AS SELECT 123")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("CREATE TABLE sc_created_after_user.zap AS SELECT 123")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT f_created_before_user()")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT f_created_after_user()")
				Expect(err).ToNot(HaveOccurred())

			})

			Context("When there are two different bindings", func() {
				var (
					otherBindingID       string
					otherCreatedUser     string
					otherCreatedPassword string
				)

				BeforeEach(func() {
					var err error
					otherBindingID = "other-binding-id" + randomTestSuffix
					otherCreatedUser, otherCreatedPassword, err = postgresEngine.CreateUser(otherBindingID, dbname, nil)
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					err := postgresEngine.DropUser(otherBindingID, dbname)
					Expect(err).ToNot(HaveOccurred())
				})

				It("CreateUser() returns different user and password", func() {
					fmt.Sprintf("created user: '%s' Other created user: '%s'", createdUser, otherCreatedUser)
					Expect(otherCreatedUser).ToNot(Equal(createdUser))
					fmt.Sprintf("created user: '%s' Other created user: '%s'", createdUser, otherCreatedUser)
					Expect(otherCreatedPassword).ToNot(Equal(createdPassword))
				})

				It("Resources created by one binding can be accessed and deleted by other", func() {
					connectionString1 := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
					connectionString2 := postgresEngine.URI(address, port, dbname, otherCreatedUser, otherCreatedPassword)
					createObjects(connectionString1, "foo")
					accessAndDeleteObjects(connectionString2, "foo")
					createObjects(connectionString2, "bar")
					accessAndDeleteObjects(connectionString1, "bar")
				})
			})
		})

		Context("With an existing user created before non-owner user support", func() {
			var (
				oldBindingID string
				oldUserUsername string
				oldUserURI string
			)

			BeforeEach(func() {
				// set up "old user", using the procedure that would have been followed in
				// previous versions
				groupName := postgresEngine.generatePostgresGroup(dbname)
				oldBindingID = utils.RandomLowerAlphaNum(6)
				oldUserUsername = postgresEngine.UsernameGenerator(oldBindingID)
				_, err := postgresEngine.db.Exec("CREATE ROLE " + groupName)
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec(fmt.Sprintf(`
	create or replace function reassign_owned() returns event_trigger language plpgsql as $$
	begin
		-- do not execute if member of rds_superuser
		IF EXISTS (select 1 from pg_catalog.pg_roles where rolname = 'rds_superuser')
		AND pg_has_role(current_user, 'rds_superuser', 'member') THEN
			RETURN;
		END IF;
		-- do not execute if not member of manager role
		IF NOT pg_has_role(current_user, '%s', 'member') THEN
			RETURN;
		END IF;
		-- do not execute if superuser
		IF EXISTS (SELECT 1 FROM pg_user WHERE usename = current_user and usesuper = true) THEN
			RETURN;
		END IF;
		EXECUTE 'reassign owned by "' || current_user || '" to "%s"';
	end
	$$;`, groupName, groupName))
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("create event trigger reassign_owned on ddl_command_end execute procedure reassign_owned();")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE USER " + oldUserUsername + " WITH PASSWORD 'mypass'")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("GRANT " + groupName + " TO " + oldUserUsername)
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("GRANT ALL PRIVILEGES ON DATABASE " + dbname + " TO " + groupName)
				Expect(err).ToNot(HaveOccurred())
				oldUserURI = postgresEngine.URI(address, port, dbname, oldUserUsername, "mypass")
			})

			It("Old and new users are able to access resources created before non-owner user support", func() {
				createObjects(oldUserURI, "foo")
				createObjects(oldUserURI, "bar")

				// ...and now we deploy non-owner user support for the first time
				newUserUsername, newUserPassword, err := postgresEngine.CreateUser(bindingID, dbname, nil)
				Expect(err).ToNot(HaveOccurred())
				newUserURI := postgresEngine.URI(address, port, dbname, newUserUsername, newUserPassword)

				accessAndDeleteObjects(oldUserURI, "foo")
				accessAndDeleteObjects(newUserURI, "bar")

				err = postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
				err = postgresEngine.DropUser(oldBindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Old users are able to access resources created by a new user", func() {
				newUserUsername, newUserPassword, err := postgresEngine.CreateUser(bindingID, dbname, nil)
				Expect(err).ToNot(HaveOccurred())
				newUserURI := postgresEngine.URI(address, port, dbname, newUserUsername, newUserPassword)

				createObjects(newUserURI, "foo")
				accessAndDeleteObjects(oldUserURI, "foo")

				err = postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
				err = postgresEngine.DropUser(oldBindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Old users can be cleanly dropped by the new DropUser() when a new CreateUser() has not yet run", func() {
				createObjects(oldUserURI, "foo")

				err := postgresEngine.DropUser(oldBindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("With invalid userBindParameters supplied", func() {
			It("Returns an error for invalid json in userBindParameters", func() {
				_, _, err := postgresEngine.CreateUser(bindingID, dbname, rawMessagePointer(`{"is_owner": true,, "grant_privileges": null}`))
				Expect(err).To(MatchError(ContainSubstring(`invalid character`)))
			})

			It("Returns an error when unable to unmarshal userBindParameters", func() {
				_, _, err := postgresEngine.CreateUser(bindingID, dbname, rawMessagePointer(`{"is_owner": 123}`))
				Expect(err).To(MatchError(ContainSubstring(`number`)))
			})

			It("Returns an error when userBindParameters fails validation", func() {
				_, _, err := postgresEngine.CreateUser(bindingID, dbname, rawMessagePointer(`{"is_owner": false, "default_privilege_policy": "grunt"}`))
				Expect(err).To(MatchError(ContainSubstring(`default_privilege_policy must be one of 'grant' or 'revoke'`)))
			})

			It("Returns an error when requesting a default-grant non-owner user on pg <10", func() {
				if pgServerVersionNum >= 100000 {
					Skip("can't test this with pg >=10")
				}

				_, _, err := postgresEngine.CreateUser(bindingID, dbname, rawMessagePointer(`{"is_owner": false, "default_privilege_policy": "grant"}`))
				Expect(err).To(MatchError(ContainSubstring(`not supported for PostgreSQL version`)))
			})
		})

		Context("With a default-revoke non-owner specified by userBindParameters", func() {
			BeforeEach(func() {
				var err error
				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_before_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE FUNCTION f_created_before_user() RETURNS integer AS 'BEGIN\nRETURN 321;\nEND;' LANGUAGE plpgsql")
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err = postgresEngine.CreateUser(
					bindingID,
					dbname,
					rawMessagePointer(
						// without public schema access very little works and it doesn't feel like we're testing much
						`{"is_owner": false, "default_privilege_policy": "revoke", "grant_privileges": [{"target_type": "SCHEMA", "target_name": "public", "privilege": "USAGE"}]}`,
					),
				)
				Expect(err).ToNot(HaveOccurred())

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_after_user AS SELECT 456 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_after_user")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE FUNCTION f_created_after_user() RETURNS integer AS 'BEGIN\nRETURN 321;\nEND;' LANGUAGE plpgsql")
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is unable to create objects", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TABLE foo (col CHAR(8))")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE SEQUENCE baz")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE SCHEMA bar")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE INDEX qux ON t_created_before_user (x)")
				Expect(err).To(MatchError(ContainSubstring(`owner`)))
			})

			It("Is unable to access objects created before the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("INSERT INTO t_created_before_user VALUES (111)")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("SELECT nextval('sq_created_before_user')")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})

			It("Is unable to access objects created after the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("INSERT INTO t_created_after_user VALUES (111)")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("SELECT nextval('sq_created_after_user')")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})

			It("Is able to call functions created before and after the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("SELECT f_created_before_user()")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT f_created_after_user()")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is unable to drop tables", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("DROP TABLE t_created_before_user")
				Expect(err).To(MatchError(ContainSubstring(`owner`)))

				_, err = db.Exec("DROP TABLE t_created_after_user")
				Expect(err).To(MatchError(ContainSubstring(`owner`)))
			})

			It("Is unable to create temporary tables", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TEMPORARY TABLE qux AS SELECT 'abc'")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})

			It("Is able to execute plpgsql", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec(fmt.Sprintf("DO %s LANGUAGE plpgsql", pq.QuoteLiteral("BEGIN\nEXECUTE 'SELECT 1';\nEND;")))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("With a default-grant non-owner specified by userBindParameters", func() {
			BeforeEach(func() {
				var err error

				if pgServerVersionNum < 100000 {
					Skip("default-grant non-owners not supported on pg <10")
				}

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_before_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE FUNCTION f_created_before_user() RETURNS integer AS 'BEGIN\nRETURN 321;\nEND;' LANGUAGE plpgsql")
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err = postgresEngine.CreateUser(
					bindingID,
					dbname,
					rawMessagePointer(
						`{"is_owner": false, "default_privilege_policy": "grant"}`,
					),
				)
				Expect(err).ToNot(HaveOccurred())

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_after_user AS SELECT 456 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_after_user")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE FUNCTION f_created_after_user() RETURNS integer AS 'BEGIN\nRETURN 321;\nEND;' LANGUAGE plpgsql")
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is unable to create objects", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TABLE foo (col CHAR(8))")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE SEQUENCE baz")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE SCHEMA bar")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE INDEX qux ON t_created_before_user (x)")
				Expect(err).To(MatchError(ContainSubstring(`owner`)))
			})

			It("Is able to access objects created before the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("INSERT INTO t_created_before_user VALUES (111)")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT nextval('sq_created_before_user')")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is able to access objects created after the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("INSERT INTO t_created_after_user VALUES (111)")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT nextval('sq_created_after_user')")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is able to call functions created before and after the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("SELECT f_created_before_user()")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT f_created_after_user()")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is unable to drop tables", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("DROP TABLE t_created_before_user")
				Expect(err).To(MatchError(ContainSubstring(`owner`)))

				_, err = db.Exec("DROP TABLE t_created_after_user")
				Expect(err).To(MatchError(ContainSubstring(`owner`)))
			})

			It("Is able to create temporary tables", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TEMPORARY TABLE qux AS SELECT 'abc'")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is able to execute plpgsql", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec(fmt.Sprintf("DO %s LANGUAGE plpgsql", pq.QuoteLiteral("BEGIN\nEXECUTE 'SELECT 1';\nEND;")))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("With a default-revoke non-owner with specific grants specified by userBindParameters", func() {
			BeforeEach(func() {
				var err error
				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_before_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec(`CREATE TABLE "t_weirdly "" ; ' ,named " AS SELECT 123 AS x`)
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err = postgresEngine.CreateUser(
					bindingID,
					dbname,
					rawMessagePointer(
						`{"is_owner": false, "default_privilege_policy": "revoke", "grant_privileges": [
							{"target_type": "SCHEMA", "target_name": "public", "privilege": "ALL"},
							{"target_type": "TABLE", "target_name": "t_created_before_user", "privilege": "SELECT"},
							{"target_type": "TABLE", "target_name": "t_created_after_user", "privilege": "SELECT"},
							{"target_type": "TABLE", "target_name": "t_weirdly \" ; ' ,named ", "privilege": "INSERT"},
							{"target_type": "TABLE", "target_name": "doesnt_exist", "privilege": "ALL"},
							{"target_type": "SEQUENCE", "target_name": "sq_created_before_user", "privilege": "SELECT"},
							{"target_type": "SEQUENCE", "target_name": "sq_created_after_user", "privilege": "ALL"},
							{"target_type": "DATABASE", "privilege": "ALL"}
						]}`,
					),
				)
				Expect(err).ToNot(HaveOccurred())

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_after_user AS SELECT 456 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_after_user")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is able to create temporary tables", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TEMPORARY TABLE qux AS SELECT 'abc'")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is granted privileges specified to pre-existing objects", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("SELECT * FROM t_created_before_user")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec(`INSERT INTO "t_weirdly "" ; ' ,named " VALUES (555)`)
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT last_value FROM sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is not granted other privileges to specified pre-existing objects", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("INSERT INTO t_created_before_user VALUES (111)")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("SELECT nextval('sq_created_before_user')")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})

			It("Is not granted privileges specified to objects created after the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("SELECT * FROM t_created_after_user")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("SELECT last_value FROM sq_created_after_user")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})

			It("Is not granted any CREATE privileges", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE SCHEMA test123")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE TABLE test456 AS SELECT 789 AS x")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})
		})

		Context("With a default-grant non-owner with specific revocations specified by userBindParameters", func() {
			BeforeEach(func() {
				var err error

				if pgServerVersionNum < 100000 {
					Skip("default-grant non-owners not supported on pg <10")
				}

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_before_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec(`CREATE TABLE "t_weirdly "" ; ' ,named " AS SELECT 123 AS x`)
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err = postgresEngine.CreateUser(
					bindingID,
					dbname,
					rawMessagePointer(
						`{"is_owner": false, "default_privilege_policy": "grant", "revoke_privileges": [
							{"target_type": "TABLE", "target_name": "t_created_before_user", "privilege": "SELECT"},
							{"target_type": "TABLE", "target_name": "t_created_after_user", "privilege": "SELECT"},
							{"target_type": "TABLE", "target_name": "t_weirdly \" ; ' ,named ", "privilege": "INSERT"},
							{"target_type": "TABLE", "target_name": "doesnt_exist", "privilege": "ALL"},
							{"target_type": "SEQUENCE", "target_name": "sq_created_before_user", "privilege": "UPDATE"},
							{"target_type": "SEQUENCE", "target_name": "sq_created_after_user", "privilege": "SELECT"}
						]}`,
					),
				)
				Expect(err).ToNot(HaveOccurred())

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_after_user AS SELECT 456 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_after_user")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is able to create temporary tables", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE TEMPORARY TABLE qux AS SELECT 'abc'")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is revoked privileges specified from pre-existing objects", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("SELECT * FROM t_created_before_user")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec(`INSERT INTO "t_weirdly "" ; ' ,named " VALUES (555)`)
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("SELECT setval('sq_created_before_user', 101)")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})

			It("Is not revoked other privileges from specified pre-existing objects", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("INSERT INTO t_created_before_user VALUES (111)")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT last_value FROM sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is not revoked privileges specified from objects created after the user", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("SELECT * FROM t_created_after_user")
				Expect(err).ToNot(HaveOccurred())

				_, err = db.Exec("SELECT last_value FROM sq_created_after_user")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Is not granted any CREATE privileges", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				Expect(err).ToNot(HaveOccurred())
				defer db.Close()

				_, err = db.Exec("CREATE SCHEMA test123")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))

				_, err = db.Exec("CREATE TABLE test456 AS SELECT 789 AS x")
				Expect(err).To(MatchError(ContainSubstring(`permission`)))
			})
		})
	})

	Describe("DropUser", func() {
		var (
			bindingID       string
			createdUser     string
			createdPassword string
		)

		BeforeEach(func() {
			bindingID = "binding-id" + randomTestSuffix
			err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("A user exists", func() {

			BeforeEach(func() {
				var err error
				createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname, nil)
				Expect(err).ToNot(HaveOccurred())
			})

			It("DropUser() removes the credentials", func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())

				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				defer db.Close()
				Expect(err).ToNot(HaveOccurred())
				err = db.Ping()
				Expect(err).To(HaveOccurred())

				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(SatisfyAny(
					BeEquivalentTo("28P01"),
					BeEquivalentTo("28000"),
				))
				Expect(pqErr.Message).To(SatisfyAny(
					MatchRegexp("authentication failed for user"),
					MatchRegexp("role .* does not exist"),
				))
			})

			It("Errors dropping the user are returned", func() {
				// other than 'role does not exist' - see below

				rootConnection, err := sql.Open("postgres", template1ConnectionString)
				defer rootConnection.Close()
				Expect(err).ToNot(HaveOccurred())
				_, err = rootConnection.Exec(fmt.Sprintf("ALTER USER %s NOSUPERUSER", masterUsername))
				Expect(err).ToNot(HaveOccurred())
				// not even being able to connect to the db is a very bland error
				_, err = rootConnection.Exec(fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", dbname, masterUsername))
				Expect(err).ToNot(HaveOccurred())

				err = postgresEngine.DropUser(bindingID, dbname)
				Expect(err).To(HaveOccurred())
				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(BeEquivalentTo("42501"))
				Expect(pqErr.Message).To(MatchRegexp("permission denied to drop role"))
			})
		})

		Context("A non-owner user exists", func() {

			BeforeEach(func() {
				var err error

				_, err = postgresEngine.db.Exec("CREATE TABLE t_created_before_user AS SELECT 123 AS x")
				Expect(err).ToNot(HaveOccurred())
				_, err = postgresEngine.db.Exec("CREATE SEQUENCE sq_created_before_user")
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err = postgresEngine.CreateUser(
					bindingID,
					dbname,
					rawMessagePointer(
						`{"is_owner": false, "default_privilege_policy": "revoke", "grant_privileges": [
							{"target_type": "SCHEMA", "target_name": "public", "privilege": "USAGE"},
							{"target_type": "TABLE", "target_name": "t_created_before_user", "privilege": "ALL"},
							{"target_type": "SEQUENCE", "target_name": "sq_created_before_user", "privilege": "ALL"}
						]}`,
					),
				)
				Expect(err).ToNot(HaveOccurred())
			})

			It("DropUser() removes the credentials", func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())

				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				defer db.Close()
				Expect(err).ToNot(HaveOccurred())
				err = db.Ping()
				Expect(err).To(HaveOccurred())

				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(SatisfyAny(
					BeEquivalentTo("28P01"),
					BeEquivalentTo("28000"),
				))
				Expect(pqErr.Message).To(SatisfyAny(
					MatchRegexp("authentication failed for user"),
					MatchRegexp("role .* does not exist"),
				))
			})
		})

		Context("A user doesn't exist", func() {
			It("Calling DropUser() doesn't fail with 'role does not exist'", func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("A user exists with a username generated the old way", func() {

			BeforeEach(func() {
				var err error
				postgresEngine.UsernameGenerator = generateUsernameOld
				createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname, nil)
				postgresEngine.UsernameGenerator = generateUsername
				Expect(err).ToNot(HaveOccurred())
			})

			It("DropUser() removes the credentials", func() {
				err := postgresEngine.DropUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())

				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				defer db.Close()
				Expect(err).ToNot(HaveOccurred())
				err = db.Ping()
				Expect(err).To(HaveOccurred())

				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(SatisfyAny(
					BeEquivalentTo("28P01"),
					BeEquivalentTo("28000"),
				))
				Expect(pqErr.Message).To(SatisfyAny(
					MatchRegexp("authentication failed for user"),
					MatchRegexp("role .* does not exist"),
				))
			})

		})

	})

	Describe("ResetState", func() {
		var (
			bindingID       string
			createdUser     string
			createdPassword string
		)

		BeforeEach(func() {
			bindingID = "binding-id" + randomTestSuffix
			err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
			Expect(err).ToNot(HaveOccurred())
		})

		Describe("when there was no user created", func() {
			It("CreateUser() can be called after ResetState()", func() {
				err := postgresEngine.ResetState()
				Expect(err).ToNot(HaveOccurred())
				_, _, err = postgresEngine.CreateUser(bindingID, dbname, nil)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Describe("when there was already a user created", func() {
			AssertState := func(){
				It("ResetState() removes the credentials", func() {
					connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
					db, err := sql.Open("postgres", connectionString)
					defer db.Close()
					Expect(err).ToNot(HaveOccurred())
					err = db.Ping()
					Expect(err).To(HaveOccurred())

					pqErr, ok := err.(*pq.Error)
					Expect(ok).To(BeTrue())
					Expect(pqErr.Code).To(SatisfyAny(
						BeEquivalentTo("28P01"),
						BeEquivalentTo("28000"),
					))
					Expect(pqErr.Message).To(SatisfyAny(
						MatchRegexp("authentication failed for user"),
						MatchRegexp("role .* does not exist"),
					))
				})

				It("CreateUser() returns the same user and different password", func() {
					user, password, err := postgresEngine.CreateUser(bindingID, dbname, nil)
					Expect(err).ToNot(HaveOccurred())
					Expect(user).To(Equal(createdUser))
					Expect(password).ToNot(Equal(createdPassword))
				})
			}

			Describe("with empty userBindParameters", func() {
				BeforeEach(func() {
					var err error
					createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname, nil)
					Expect(err).ToNot(HaveOccurred())

					err = postgresEngine.ResetState()
					Expect(err).ToNot(HaveOccurred())
				})

				AssertState()
			})

			Describe("with non-owner default-revoke userBindParameters", func() {
				BeforeEach(func() {
					var err error
					createdUser, createdPassword, err = postgresEngine.CreateUser(
						bindingID,
						dbname,
						rawMessagePointer(
							`{"is_owner": false, "default_privilege_policy": "revoke"}`,
						),
					)
					Expect(err).ToNot(HaveOccurred())

					err = postgresEngine.ResetState()
					Expect(err).ToNot(HaveOccurred())
				})

				AssertState()
			})

			Describe("with non-owner default-grant userBindParameters", func() {
				BeforeEach(func() {

					if pgServerVersionNum < 100000 {
						Skip("default-grant non-owners not supported on pg <10")
					}

					var err error
					createdUser, createdPassword, err = postgresEngine.CreateUser(
						bindingID,
						dbname,
						rawMessagePointer(
							`{"is_owner": false, "default_privilege_policy": "grant"}`,
						),
					)
					Expect(err).ToNot(HaveOccurred())

					err = postgresEngine.ResetState()
					Expect(err).ToNot(HaveOccurred())
				})

				AssertState()
			})
		})
	})

	Describe("Extensions", func() {
		It("can create and drop extensions", func() {
			By("creating the extensions")
			err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
			defer postgresEngine.Close()
			Expect(err).ToNot(HaveOccurred())
			err = postgresEngine.CreateExtensions([]string{"uuid-ossp", "pgcrypto"})
			Expect(err).ToNot(HaveOccurred())
			rows, err := postgresEngine.db.Query("SELECT extname FROM pg_catalog.pg_extension")
			defer rows.Close()
			Expect(err).ToNot(HaveOccurred())

			By("checking the extensions post CreateExtensions")
			extensions := []string{}
			for rows.Next() {
				var name string
				err = rows.Scan(&name)
				Expect(err).ToNot(HaveOccurred())
				extensions = append(extensions, name)
			}
			Expect(rows.Err()).ToNot(HaveOccurred())
			Expect(extensions).To(ContainElement("uuid-ossp"))
			Expect(extensions).To(ContainElement("pgcrypto"))

			By("dropping the extensions")
			err = postgresEngine.DropExtensions([]string{"pgcrypto"})
			Expect(err).ToNot(HaveOccurred())
			rows, err = postgresEngine.db.Query("SELECT extname FROM pg_catalog.pg_extension")
			defer rows.Close()
			Expect(err).ToNot(HaveOccurred())

			By("checking the extensions post DropExtensions")
			extensions = []string{}
			for rows.Next() {
				var name string
				err = rows.Scan(&name)
				Expect(err).ToNot(HaveOccurred())
				extensions = append(extensions, name)
			}
			Expect(rows.Err()).ToNot(HaveOccurred())
			Expect(extensions).To(ContainElement("uuid-ossp"))
			Expect(extensions).ToNot(ContainElement("pgcrypto"))
		})
	})
})
