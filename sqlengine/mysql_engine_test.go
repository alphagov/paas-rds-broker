package sqlengine

import (
	"database/sql"
	"strconv"

	"github.com/alphagov/paas-rds-broker/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"

	"code.cloudfoundry.org/lager"
)

func createMysqlDB(connectionString, dbName string) {
	db, err := sql.Open("mysql", connectionString)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

	statement := "CREATE DATABASE " + dbName
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())
}

func dropMysqlDB(connectionString, dbName string) {
	db, err := sql.Open("mysql", connectionString)
	Expect(err).ToNot(HaveOccurred())
	defer db.Close()

	statement := "DROP DATABASE " + dbName
	_, err = db.Exec(statement)
	Expect(err).ToNot(HaveOccurred())
}

var _ = Describe("MySQLEngine", func() {
	var (
		mysqlEngine *MySQLEngine
		logger      lager.Logger

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

		mysqlEngine = NewMySQLEngine(logger)
		mysqlEngine.requireSSL = false

		address = getEnvOrDefault("MYSQL_HOSTNAME", "127.0.0.1")
		portString := getEnvOrDefault("MYSQL_PORT", "3306")
		p, err := strconv.Atoi(portString)
		Expect(err).ToNot(HaveOccurred())
		port = int64(p)

		username = getEnvOrDefault("MYSQL_USERNAME", "root")
		password = getEnvOrDefault("MYSQL_PASSWORD", "")

		dbname = "mydb" + randomTestSuffix

		template1ConnectionString = mysqlEngine.connectionString(address, port, "", username, password)

		fmt.Println(template1ConnectionString)

		// Create the test DB
		createMysqlDB(template1ConnectionString, dbname)
	})

	AfterEach(func() {
		mysqlEngine.Close() // Ensure the DB is closed
		dropMysqlDB(template1ConnectionString, dbname)
	})

	It("can connect to the new DB", func() {
		err := mysqlEngine.Open(address, port, dbname, username, password)
		defer mysqlEngine.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns error if engine is the database is not reachable", func() {
		err := mysqlEngine.Open("localhost", 1, dbname, username, password)
		defer mysqlEngine.Close()
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
			err := mysqlEngine.Open(address, port, dbname, username, password)
			Expect(err).ToNot(HaveOccurred())
			createdUser, createdPassword, err = mysqlEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			// dropMysqlDB(template1ConnectionString, dbname)
		})

		It("CreateUser() returns the same user and password when called several times", func() {
			user, password, err := mysqlEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
			Expect(user).To(Equal(createdUser))
			Expect(password).To(Equal(createdPassword))
			user, password, err = mysqlEngine.CreateUser(bindingID, dbname)
			Expect(err).ToNot(HaveOccurred())
			Expect(user).To(Equal(createdUser))
			Expect(password).To(Equal(createdPassword))
		})
	})

	Describe("ResetState", func() {
		var (
			bindingID       string
			createdUser     string
			createdPassword string
		)
		BeforeEach(func() {
			bindingID = "binding-id"
			err := mysqlEngine.Open(address, port, dbname, username, password)
			Expect(err).ToNot(HaveOccurred())
		})
		Describe("when there was no user created", func() {
			AfterEach(func() {
				// dropMysqlDB(template1ConnectionString, dbname)
			})

			It("get login and password when calling CreateUser() ", func() {
				err := mysqlEngine.ResetState()
				Expect(err).ToNot(HaveOccurred())
				_, _, err = mysqlEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})
		})
		Describe("when there was already a user created", func() {
			BeforeEach(func() {
				var err error
				createdUser, createdPassword, err = mysqlEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				// dropMysqlDB(template1ConnectionString, dbname)
			})

			It("CreateUser() returns the same user but different password", func() {
				err := mysqlEngine.ResetState()
				Expect(err).ToNot(HaveOccurred())
				user, password, err := mysqlEngine.CreateUser(bindingID, dbname)
				Expect(err).ToNot(HaveOccurred())
				Expect(user).To(Equal(createdUser))
				Expect(password).ToNot(Equal(createdPassword))
			})
		})
	})

})
