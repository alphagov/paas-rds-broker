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

					_, _, err = postgresEngine.CreateUser(bindingID, dbname)
					Expect(err).ToNot(HaveOccurred())

					err = postgresEngine.DropUser(bindingID)
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

		Context("A user doesn't exist", func() {
			It("Calling DropUser() doesn't fail with 'role does not exist'", func() {
				err := postgresEngine.DropUser(bindingID)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("A user exists with a username generated the old way", func() {

			BeforeEach(func() {
				var err error
				postgresEngine.UsernameGenerator = generateUsernameOld
				createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname)
				postgresEngine.UsernameGenerator = generateUsername
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
				user, password, err := postgresEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
				Expect(user).To(Equal(createdUser))
				Expect(password).ToNot(Equal(createdPassword))
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

	Describe("Executor", func() {
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

		It("executes SQL", func() {
			connectionString := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
			db, err := sql.Open("postgres", connectionString)
			Expect(err).ToNot(HaveOccurred())
			defer db.Close()

			err = postgresEngine.ExecuteStatement("SELECT 1;")
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
