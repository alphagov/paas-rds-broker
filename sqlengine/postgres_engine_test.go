package sqlengine

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/template"

	"github.com/alphagov/paas-rds-broker/utils"
	"github.com/lib/pq"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/lager"
	"bytes"
)

func createMasterUser(connectionString string) (string, string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	randomMasterUser := "master_" + utils.RandomLowerAlphaNum(6)
	password := "mypass"

	statement := "CREATE USER " + randomMasterUser + " UNENCRYPTED PASSWORD '" + password + "'"
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

	statement := "DROP OWNED BY " + username
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

func createLegacyUser(connectionString, dbname string) (string, string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	username := dbname + "_owner"
	password := "mypass"

	createUserStatement := "CREATE USER \"" + username + "\" WITH PASSWORD '" + password + "';"
	_, err = db.Exec(createUserStatement)
	Expect(err).ToNot(HaveOccurred())

	grantPrivilegesStatement := "GRANT ALL PRIVILEGES ON DATABASE \"" + dbname + "\" TO \"" + username + "\";"
	_, err = db.Exec(grantPrivilegesStatement)
	Expect(err).ToNot(HaveOccurred())

	return username, password
}

func migrationTest(connectionString, dbname string) {

	user := dbname + "_owner"
	groupname := dbname + "_manager"

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

	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	var ensureGroupStatement bytes.Buffer
	if err := ensureGroupTemplate.Execute(&ensureGroupStatement, map[string]string{
		"role": groupname,
	}); err != nil {
		return
	}

	if _, err := db.Exec(ensureGroupStatement.String()); err != nil {

		return
	}

	var ensureTriggerStatement bytes.Buffer
	if err := ensureTriggerTemplate.Execute(&ensureTriggerStatement, map[string]string{
		"role": groupname,
	}); err != nil {
		return
	}

	cmds := []string{
		ensureTriggerStatement.String(),
		`drop event trigger if exists reassign_owned;`,
		`create event trigger reassign_owned on ddl_command_end execute procedure reassign_owned();`,
	}

	for _, cmd := range cmds {
		_, err = db.Exec(cmd)
		if err != nil {
			return
		}
	}

	usersStatement := "SELECT usename FROM pg_user WHERE usename LIKE '%owner';"
	_, err = db.Exec(usersStatement)
	Expect(err).ToNot(HaveOccurred())

	grantPrivilegesStatement := "grant " + groupname + " to " + user + ";"
	_, err = db.Exec(grantPrivilegesStatement)
	Expect(err).ToNot(HaveOccurred())

	reassignStatement := "reassign owned by " + user + " to " + groupname + ";"
	_, err = db.Exec(reassignStatement)
	Expect(err).ToNot(HaveOccurred())

}

func createObjects(connectionString, tableName string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	_, err = db.Exec("CREATE TABLE " + tableName + "(col CHAR(8))")
	Expect(err).ToNot(HaveOccurred())

	_, err = db.Exec("INSERT INTO " + tableName + " (col) VALUES ('value')")
	Expect(err).ToNot(HaveOccurred())
}

func accessAndDeleteObjects(connectionString, tableName string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	var col string
	err = db.QueryRow("SELECT * FROM " + tableName + " WHERE col = 'value'").Scan(&col)
	Expect(err).ToNot(HaveOccurred())
	Expect(strings.TrimSpace(col)).To(BeEquivalentTo("value"))

	_, err = db.Exec("DROP TABLE " + tableName)
	Expect(err).ToNot(HaveOccurred())
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
		masterConnectionString    string
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
		masterConnectionString = postgresEngine.URI(address, port, dbname, masterUsername, masterPassword)

		// Create the test DB
		createDB(template1ConnectionString, dbname)
	})

	AfterEach(func() {
		postgresEngine.Close() // Ensure the DB is closed
		dropDB(template1ConnectionString, dbname)
		dropTestUser(template1ConnectionString, masterUsername)
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

			createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			err := postgresEngine.DropUser(bindingID)
			Expect(err).ToNot(HaveOccurred())
		})

		It("CreateUser() returns valid credentials", func() {
			connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
			db, err := sql.Open("postgres", connectionString)
			defer db.Close()
			Expect(err).ToNot(HaveOccurred())
			err = db.Ping()
			Expect(err).ToNot(HaveOccurred())
		})

		It("CreateUser() fails when called several times with the same bindingID", func() {
			_, _, err := postgresEngine.CreateUser(bindingID, dbname)
			Expect(err).To(HaveOccurred())
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
				otherCreatedUser, otherCreatedPassword, err = postgresEngine.CreateUser(otherBindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := postgresEngine.DropUser(otherBindingID)
				Expect(err).ToNot(HaveOccurred())
			})

			It("CreateUser() returns different user and password", func() {
				fmt.Sprintf("created user: '%s' Other created user: '%s'", createdUser, otherCreatedUser)
				Expect(otherCreatedUser).ToNot(Equal(createdUser))
				fmt.Sprintf("created user: '%s' Other created user: '%s'", createdUser, otherCreatedUser)
				Expect(otherCreatedPassword).ToNot(Equal(createdPassword))
			})

			It("Tables created by one binding can be accessed and deleted by other", func() {
				connectionString1 := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				connectionString2 := postgresEngine.URI(address, port, dbname, otherCreatedUser, otherCreatedPassword)
				createObjects(connectionString1, "table1")
				accessAndDeleteObjects(connectionString2, "table1")
				createObjects(connectionString2, "table2")
				accessAndDeleteObjects(connectionString1, "table2")
			})
		})
	})

	Describe("MigrateLegacyUsers", func() {
		var (
			bindingID              string
			connectionStringNew    string
			connectionStringLegacy string
			legacyUserName         string
			legacyUserPassword     string
		)

		Context("There is a user from a legacy binding", func() {
			BeforeEach(func() {
				legacyUserName, legacyUserPassword = createLegacyUser(masterConnectionString, dbname)
				connectionStringLegacy = postgresEngine.URI(address, port, dbname, legacyUserName, legacyUserPassword)
				createObjects(connectionStringLegacy, "legacy_table")
			})

			BeforeEach(func() {
				bindingID = "binding-id" + randomTestSuffix
				err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
				Expect(err).ToNot(HaveOccurred())

				createdUser, createdPassword, err := postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())

				connectionStringNew = postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
			})

			AfterEach(func() {
				err := postgresEngine.DropUser(bindingID)
				Expect(err).ToNot(HaveOccurred())
				dropTestUser(masterConnectionString, legacyUserName)
			})

			It("Should allow the new bindings to access objects created by the legacy user", func() {
				accessAndDeleteObjects(connectionStringNew, "legacy_table")
			})

			It("Should allow the legacy user to access objects from new bindings", func() {
				createObjects(connectionStringNew, "new_table")
				accessAndDeleteObjects(connectionStringLegacy, "new_table")
			})

		})

	})

	Describe("Migrate bindings", func() {
		var (
			bindingID              string
			connectionStringNew    string
			connectionStringLegacy string
			legacyUserName         string
			legacyUserPassword     string
			createdUser            string
			createdPassword        string
			otherCreatedUser       string
			otherCreatedPassword   string
		)

		Context("Migrate legacy databases", func() {
			BeforeEach(func() {
				legacyUserName, legacyUserPassword = createLegacyUser(masterConnectionString, dbname)
				connectionStringLegacy = postgresEngine.URI(address, port, dbname, legacyUserName, legacyUserPassword)
				createObjects(connectionStringLegacy, "legacy_table")
				bindingID = "binding-id" + randomTestSuffix
				err := postgresEngine.Open(address, port, dbname, masterUsername, masterPassword)
				Expect(err).ToNot(HaveOccurred())
				createdUser, createdPassword, err := postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
				connectionStringNew = postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
			})

			AfterEach(func() {
				err := postgresEngine.DropUser(bindingID)
				Expect(err).ToNot(HaveOccurred())
				dropTestUser(masterConnectionString, legacyUserName)
				dropTestUser(masterConnectionString, createdUser)
				dropTestUser(masterConnectionString, otherCreatedUser)
			})

			It("Migrate legacy databases with one user", func() {
				migrationTest(connectionStringLegacy, dbname)
				createObjects(connectionStringLegacy, "table1")
				accessAndDeleteObjects(connectionStringLegacy, "table1")

			})

			It("Migrate legacy databases with multiple binds", func() {
				migrationTest(connectionStringLegacy, dbname)
				connectionString1 := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				connectionString2 := postgresEngine.URI(address, port, dbname, otherCreatedUser, otherCreatedPassword)
				createObjects(connectionString1, "table1")
				accessAndDeleteObjects(connectionString2, "table1")
				createObjects(connectionString2, "table2")
				accessAndDeleteObjects(connectionString1, "table2")
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
				createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			It("DropUser() removes the credentials", func() {
				err := postgresEngine.DropUser(bindingID)
				Expect(err).ToNot(HaveOccurred())

				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				defer db.Close()
				Expect(err).ToNot(HaveOccurred())
				err = db.Ping()
				Expect(err).To(HaveOccurred())

				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(BeEquivalentTo("28000"))
				Expect(pqErr.Message).To(MatchRegexp("role .* does not exist"))
			})

			It("Calling DropUser() twice doesn't fail with 'role does not exist'", func() {
				err := postgresEngine.DropUser(bindingID)
				Expect(err).ToNot(HaveOccurred())
				err = postgresEngine.DropUser(bindingID)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Other errors are not ignored", func() {
				rootConnection, err := sql.Open("postgres", template1ConnectionString)
				defer rootConnection.Close()
				Expect(err).ToNot(HaveOccurred())
				revoke := "ALTER USER " + masterUsername + " NOSUPERUSER"
				_, err = rootConnection.Exec(revoke)
				Expect(err).ToNot(HaveOccurred())

				err = postgresEngine.DropUser(bindingID)
				Expect(err).To(HaveOccurred())
				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(BeEquivalentTo("42501"))
				Expect(pqErr.Message).To(MatchRegexp("permission denied to drop role"))
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
				_, _, err = postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Describe("when there was already a user created", func() {
			BeforeEach(func() {
				var err error
				createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())

				err = postgresEngine.ResetState()
				Expect(err).ToNot(HaveOccurred())
			})

			It("ResetState() removes the credentials", func() {
				connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db, err := sql.Open("postgres", connectionString)
				defer db.Close()
				Expect(err).ToNot(HaveOccurred())
				err = db.Ping()
				Expect(err).To(HaveOccurred())

				pqErr, ok := err.(*pq.Error)
				Expect(ok).To(BeTrue())
				Expect(pqErr.Code).To(BeEquivalentTo("28000"))
				Expect(pqErr.Message).To(MatchRegexp("role .* does not exist"))
			})

			It("CreateUser() returns the same user and different password", func() {
				user, password, err := postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
				Expect(user).To(Equal(createdUser))
				Expect(password).ToNot(Equal(createdPassword))
			})

		})
	})

})
