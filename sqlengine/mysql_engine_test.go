package sqlengine

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/alphagov/paas-rds-broker/utils"
	"github.com/go-sql-driver/mysql"
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
		masterUsername string
		masterPassword string

		randomTestSuffix string

		template1ConnectionString string
	)

	BeforeEach(func() {
		logger = lager.NewLogger("provider_service_test")
		logger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		randomTestSuffix = "_" + utils.RandomLowerAlphaNum(6)

		mysqlEngine = NewMySQLEngine(logger)
		mysqlEngine.requireSSL = false

		address = getEnvOrDefault("MYSQL_HOSTNAME", "127.0.0.1")
		portString := getEnvOrDefault("MYSQL_PORT", "3307")
		p, err := strconv.Atoi(portString)
		Expect(err).ToNot(HaveOccurred())
		port = int64(p)

		masterUsername = getEnvOrDefault("MYSQL_USERNAME", "root")
		masterPassword = getEnvOrDefault("MYSQL_PASSWORD", "")

		dbname = "mydb" + randomTestSuffix

		template1ConnectionString = mysqlEngine.connectionString(address, port, "", masterUsername, masterPassword)

		// Create the test DB
		createMysqlDB(template1ConnectionString, dbname)
	})

	AfterEach(func() {
		mysqlEngine.Close() // Ensure the DB is closed
		dropMysqlDB(template1ConnectionString, dbname)
	})

	It("can connect to the new DB", func() {
		err := mysqlEngine.Open(address, port, dbname, masterUsername, masterPassword)
		defer mysqlEngine.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns error if engine is the database is not reachable", func() {
		err := mysqlEngine.Open("localhost", 1, dbname, masterUsername, masterPassword)
		defer mysqlEngine.Close()
		Expect(err).To(HaveOccurred())
	})

	It("returns error LoginFailedError if the credentials are wrong", func() {
		err := mysqlEngine.Open(address, port, dbname, masterUsername, "wrong_password")
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
			err := mysqlEngine.Open(address, port, dbname, masterUsername, masterPassword)
			Expect(err).ToNot(HaveOccurred())
		})

		It("CreateUser() should successfully complete it's destiny", func() {
			createdUser, createdPassword, err := mysqlEngine.CreateUser(bindingID, dbname, false)
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

			_, _, err := mysqlEngine.CreateUser(bindingID, dbname, false)
			Expect(err).ToNot(HaveOccurred())

			mysqlEngine.UsernameGenerator = generateUsername

			err = mysqlEngine.DropUser(bindingID)
			Expect(err).ToNot(HaveOccurred())
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
			err := mysqlEngine.Open(address, port, dbname, masterUsername, masterPassword)
			Expect(err).ToNot(HaveOccurred())
		})

		Describe("when there was no user created", func() {
			It("CreateUser() can be called after ResetState()", func() {
				err := mysqlEngine.ResetState()
				Expect(err).ToNot(HaveOccurred())
				_, _, err = mysqlEngine.CreateUser(bindingID, dbname, false)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Describe("when there was already a user created", func() {
			BeforeEach(func() {
				var err error
				createdUser, createdPassword, err = mysqlEngine.CreateUser(bindingID, dbname, false)
				Expect(err).ToNot(HaveOccurred())

				err = mysqlEngine.ResetState()
				Expect(err).ToNot(HaveOccurred())
			})

			It("ResetState() removes the credentials", func() {
				connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", createdUser, createdPassword, address, port, dbname)
				db, err := sql.Open("mysql", connectionString)
				defer db.Close()
				Expect(err).ToNot(HaveOccurred())
				err = db.Ping()
				Expect(err).To(HaveOccurred())

				myErr, ok := err.(*mysql.MySQLError)
				Expect(ok).To(BeTrue())
				Expect(myErr.Number).To(SatisfyAny(
					BeEquivalentTo(1045),
				))
				Expect(myErr.Message).To(SatisfyAny(
					MatchRegexp("Access denied.*"),
				))
			})

			It("CreateUser() returns the same user and different password", func() {
				user, password, err := mysqlEngine.CreateUser(bindingID, dbname, false)
				Expect(err).ToNot(HaveOccurred())
				Expect(user).To(Equal(createdUser))
				Expect(password).ToNot(Equal(createdPassword))
			})

		})
	})

})
