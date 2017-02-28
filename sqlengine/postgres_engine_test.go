package sqlengine

import (
	"database/sql"
	"os"
	"strconv"
	"strings"

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

	statement := "CREATE USER " + randomMasterUser + " UNENCRYPTED PASSWORD '" + password + "'"
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())

	statement = "ALTER USER " + randomMasterUser + " WITH SUPERUSER"
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())

	return randomMasterUser, password
}

func dropMasterUser(connectionString, randomMasterUser string) {
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()
	Expect(err).ToNot(HaveOccurred())

	statement := "DROP USER " + randomMasterUser
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())
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
	})

	BeforeEach(func() {
		// Create the test DB
		createDB(template1ConnectionString, dbname)
	})

	AfterEach(func() {
		postgresEngine.Close() // Ensure the DB is closed
		dropDB(template1ConnectionString, dbname)
		dropMasterUser(template1ConnectionString, masterUsername)
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
				Expect(otherCreatedUser).ToNot(Equal(createdUser))
				Expect(otherCreatedPassword).ToNot(Equal(createdPassword))
			})

			It("Tables created by one binding can be accessed and deleted by other", func() {
				connectionString1 := postgresEngine.URI(address, port, dbname, createdUser, createdPassword)
				db1, err := sql.Open("postgres", connectionString1)
				defer db1.Close()
				Expect(err).ToNot(HaveOccurred())

				connectionString2 := postgresEngine.URI(address, port, dbname, otherCreatedUser, otherCreatedPassword)
				db2, err := sql.Open("postgres", connectionString2)
				defer db2.Close()
				Expect(err).ToNot(HaveOccurred())

				_, err = db1.Exec("CREATE TABLE a_table(col CHAR(8))")
				Expect(err).ToNot(HaveOccurred())

				_, err = db1.Exec("INSERT INTO a_table (col) VALUES ('value')")
				Expect(err).ToNot(HaveOccurred())

				var col string
				err = db2.QueryRow("SELECT * FROM a_table WHERE col = 'value'").Scan(&col)
				Expect(err).ToNot(HaveOccurred())
				Expect(strings.TrimSpace(col)).To(BeEquivalentTo("value"))

				_, err = db2.Exec("DROP TABLE a_table")
				Expect(err).ToNot(HaveOccurred())
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
