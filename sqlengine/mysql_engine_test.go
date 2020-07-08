package sqlengine

import (
	"database/sql"
	"strconv"

	"github.com/alphagov/paas-rds-broker/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

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

		readOnlyUser = false
	)

	BeforeEach(func() {
		logger = lager.NewLogger("provider_service_test")
		logger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		randomTestSuffix := "_" + utils.RandomLowerAlphaNum(6)

		mysqlEngine = NewMySQLEngine(logger)
		mysqlEngine.requireSSL = false

		address = getEnvOrDefault("MYSQL_HOSTNAME", "127.0.0.1")
		portString := getEnvOrDefault("MYSQL_PORT", "3307")
		p, err := strconv.Atoi(portString)
		Expect(err).ToNot(HaveOccurred())
		port = int64(p)

		username = getEnvOrDefault("MYSQL_USERNAME", "root")
		password = getEnvOrDefault("MYSQL_PASSWORD", "")

		dbname = "mydb" + randomTestSuffix

		template1ConnectionString = mysqlEngine.connectionString(address, port, "", username, password)

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

	It("returns error LoginFailedError if the credentials are wrong", func() {
		err := mysqlEngine.Open(address, port, dbname, username, "wrong_password")
		defer mysqlEngine.Close()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(LoginFailedError))
	})

	Describe("User modification tests", func() {
		var (
			bindingID string
		)

		BeforeEach(func() {
			bindingID = "binding-id"
			err := mysqlEngine.Open(address, port, dbname, username, password)
			Expect(err).ToNot(HaveOccurred())
		})

		It("CreateUser() should successfully complete it's destiny", func() {
			createdUser, createdPassword, err := mysqlEngine.CreateUser(bindingID, dbname, username, &readOnlyUser)
			Expect(err).ToNot(HaveOccurred())
			Expect(createdUser).NotTo(BeEmpty())
			Expect(createdPassword).NotTo(BeEmpty())

			By("should connect to the DB with createdUser")

			err = mysqlEngine.Open(address, port, dbname, createdUser, createdPassword)
			Expect(err).ToNot(HaveOccurred())
		})

		It("DropUser() should drop the user successfully", func() {
			err := mysqlEngine.DropUser(bindingID)
			Expect(err).ToNot(HaveOccurred())
		})

		It("DropUser() should drop the username generated the old way successfully", func() {
			mysqlEngine.UsernameGenerator = generateUsernameOld

			_, _, err := mysqlEngine.CreateUser(bindingID, dbname, username, &readOnlyUser)
			Expect(err).ToNot(HaveOccurred())

			mysqlEngine.UsernameGenerator = generateUsername

			err = mysqlEngine.DropUser(bindingID)
			Expect(err).ToNot(HaveOccurred())
		})
	})

})
