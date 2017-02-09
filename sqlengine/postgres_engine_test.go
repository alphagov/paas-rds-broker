package sqlengine

import (
	"database/sql"
	"os"
	"strconv"

	"github.com/alphagov/paas-rds-broker/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/pivotal-golang/lager"
)

func createDB(connectionString, dbName string) {
	db, err := sql.Open("postgres", connectionString)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

	statement := "CREATE DATABASE " + dbName
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())
}

func dropDB(connectionString, dbName string) {
	db, err := sql.Open("postgres", connectionString)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

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

		address  string
		port     int64
		dbname   string
		username string
		password string

		template1ConnectionString string
	)

	BeforeEach(func() {
		logger = lager.NewLogger("provider_service_test")
		logger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		randomTestSuffix := "_" + utils.RandomLowerAlphaNum(6)

		postgresEngine = NewPostgresEngine(logger, "encryption key")
		postgresEngine.requireSSL = false
		postgresEngine.stateDBName = defaultStateDBName + randomTestSuffix

		address = getEnvOrDefault("POSTGRESQL_HOSTNAME", "localhost")
		portString := getEnvOrDefault("POSTGRESQL_PORT", "5432")
		p, err := strconv.Atoi(portString)
		Expect(err).ToNot(HaveOccurred())
		port = int64(p)

		username = getEnvOrDefault("POSTGRESQL_USERNAME", "postgres")
		password = getEnvOrDefault("POSTGRESQL_PASSWORD", "")

		dbname = "mydb" + randomTestSuffix

		template1ConnectionString = postgresEngine.URI(address, port, "template1", username, password)
	})

	BeforeEach(func() {
		// Create the test DB
		createDB(template1ConnectionString, dbname)
	})

	AfterEach(func() {
		postgresEngine.Close() // Ensure the DB is closed
		dropDB(template1ConnectionString, dbname)
	})

	It("can connect to the new DB", func() {
		err := postgresEngine.Open(address, port, dbname, username, password)
		defer postgresEngine.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns error if engine is the database is not reachable", func() {
		err := postgresEngine.Open("localhost", 1, dbname, username, password)
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
			bindingID = "binding-id"
			err := postgresEngine.Open("localhost", port, dbname, username, password)
			Expect(err).ToNot(HaveOccurred())
			createdUser, createdPassword, err = postgresEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			dropDB(template1ConnectionString, postgresEngine.stateDBName)
		})

		It("CreateUser() returns the same user and password when called several times", func() {
			user, password, err := postgresEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
			Expect(user).To(Equal(createdUser))
			Expect(password).To(Equal(createdPassword))
			user, password, err = postgresEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
			Expect(user).To(Equal(createdUser))
			Expect(password).To(Equal(createdPassword))
		})
	})

})
