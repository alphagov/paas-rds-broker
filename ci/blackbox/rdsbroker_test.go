package integration_aws_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"

	uuid "github.com/satori/go.uuid"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

const (
	INSTANCE_CREATE_TIMEOUT = 30 * time.Minute
)

var _ = Describe("RDS Broker Daemon", func() {
	It("should check the instance credentials", func() {
		Eventually(rdsBrokerSession, 30*time.Second).Should(gbytes.Say("credentials check has ended"))
	})

	Describe("Services", func() {
		It("returns the proper CatalogResponse", func() {
			var err error

			catalog, err := brokerAPIClient.GetCatalog()
			Expect(err).ToNot(HaveOccurred())

			sort.Sort(ByServiceID(catalog.Services))

			Expect(catalog.Services).To(HaveLen(2))
			service1 := catalog.Services[0]
			service2 := catalog.Services[1]

			Expect(service1.ID).To(Equal("mysql"))
			Expect(service1.Name).To(Equal("mysql"))
			Expect(service1.Description).To(Equal("AWS RDS MySQL service"))
			Expect(service1.Bindable).To(BeTrue())
			Expect(service1.PlanUpdatable).To(BeTrue())
			Expect(service1.Plans).To(HaveLen(2))

			Expect(service2.ID).To(Equal("postgres"))
			Expect(service2.Name).To(Equal("postgres"))
			Expect(service2.Description).To(Equal("AWS RDS PostgreSQL service"))
			Expect(service2.Bindable).To(BeTrue())
			Expect(service2.PlanUpdatable).To(BeTrue())
			Expect(service2.Plans).To(HaveLen(2))
		})
	})

	Describe("Instance Provision/Bind/Deprovision", func() {
		TestProvisionBindDeprovision := func(serviceID string) {
			const planID = "micro-without-snapshot"

			var (
				instanceID string
				appGUID    string
				bindingID  string
			)

			BeforeEach(func() {
				instanceID = uuid.NewV4().String()
				appGUID = uuid.NewV4().String()
				bindingID = uuid.NewV4().String()

				brokerAPIClient.AcceptsIncomplete = true

				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))
			})

			AfterEach(func() {
				brokerAPIClient.AcceptsIncomplete = true
				code, operation, err := brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))
			})

			It("handles binding properly", func() {
				By("creating a binding")
				resp, err := brokerAPIClient.DoBindRequest(instanceID, serviceID, planID, appGUID, bindingID)
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(201))

				By("using the credentials from the binding")
				credentials, err := getCredentialsFromBindResponse(resp)
				Expect(err).ToNot(HaveOccurred())
				err = setupPermissionsTest(credentials.URI)
				Expect(err).ToNot(HaveOccurred())
				err = postgresExtensionsTest(credentials.URI)
				Expect(err).ToNot(HaveOccurred())

				By("re-binding")
				resp, err = brokerAPIClient.DoUnbindRequest(instanceID, serviceID, planID, bindingID)
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(200))
				resp, err = brokerAPIClient.DoBindRequest(instanceID, serviceID, planID, appGUID, bindingID)
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(201))

				By("using the new credentials to alter existing objects")
				credentials, err = getCredentialsFromBindResponse(resp)
				Expect(err).ToNot(HaveOccurred())
				err = permissionsTest(credentials.URI)
				Expect(err).ToNot(HaveOccurred())
			})
		}

		Describe("Postgres", func() {
			TestProvisionBindDeprovision("postgres")
		})

		Describe("MySQL", func() {
			TestProvisionBindDeprovision("mysql")
		})
	})

	Describe("Final snapshot enable/disable", func() {
		TestFinalSnapshot := func(serviceID string) {
			const planID = "micro"

			var (
				instanceID string
				appGUID    string
				bindingID  string
			)

			BeforeEach(func() {
				instanceID = uuid.NewV4().String()
				appGUID = uuid.NewV4().String()
				bindingID = uuid.NewV4().String()
				brokerAPIClient.AcceptsIncomplete = true
			})

			It("should create a final snapshot by default", func() {
				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))

				snapshots, err := rdsClient.GetDBFinalSnapshots(instanceID)
				fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshots)
				Expect(err).NotTo(HaveOccurred())
				Expect(snapshots).Should(ContainSubstring(instanceID))

				snapshotDeletionOutput, err := rdsClient.DeleteDBFinalSnapshot(instanceID)
				fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should not create a final snapshot when `skip_final_snapshot` is set at provision time", func() {
				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, `{"skip_final_snapshot": "true"}`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))

				snapshots, err := rdsClient.GetDBFinalSnapshots(instanceID)
				fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshots)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

				snapshotDeletionOutput, err := rdsClient.DeleteDBFinalSnapshot(instanceID)
				fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
				Expect(err).To(HaveOccurred())
			})

			It("should not create a final snapshot when `skip_final_snapshot` is set via update", func() {
				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.UpdateInstance(instanceID, serviceID, planID, planID, `{"skip_final_snapshot": "true"}`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))

				snapshots, err := rdsClient.GetDBFinalSnapshots(instanceID)
				fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshots)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

				snapshotDeletionOutput, err := rdsClient.DeleteDBFinalSnapshot(instanceID)
				fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
				Expect(err).To(HaveOccurred())
			})
		}

		Describe("Postgres", func() {
			TestFinalSnapshot("postgres")
		})

		Describe("MySQL", func() {
			TestFinalSnapshot("mysql")
		})
	})
})

func pollForOperationCompletion(instanceID, serviceID, planID, operation string) string {
	var state string
	var err error

	fmt.Fprint(GinkgoWriter, "Polling for Instance Operation to complete")
	Eventually(
		func() string {
			fmt.Fprint(GinkgoWriter, ".")
			state, err = brokerAPIClient.GetLastOperationState(instanceID, serviceID, planID, operation)
			Expect(err).ToNot(HaveOccurred())
			return state
		},
		INSTANCE_CREATE_TIMEOUT,
		15*time.Second,
	).Should(
		SatisfyAny(
			Equal("succeeded"),
			Equal("failed"),
			Equal("gone"),
		),
	)

	fmt.Fprintf(GinkgoWriter, "done. Final state: %s.\n", state)
	return state
}

type bindingResponse struct {
	Credentials rdsbroker.Credentials `json:"credentials"`
}

func getCredentialsFromBindResponse(resp *http.Response) (*rdsbroker.Credentials, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	bindingResponse := bindingResponse{}
	err = json.Unmarshal(body, &bindingResponse)
	if err != nil {
		return nil, err
	}

	return &bindingResponse.Credentials, nil
}

func openConnection(databaseURI string) (*sql.DB, error) {
	dbURL, err := url.Parse(databaseURI)
	if err != nil {
		return nil, err
	}

	var dsn string
	switch dbURL.Scheme {
	case "postgres":
		dsn = dbURL.String()
	case "mysql":
		dsn = fmt.Sprintf("%s@tcp(%s)%s?tls=true",
			dbURL.User.String(),
			dbURL.Host,
			dbURL.EscapedPath(),
		)
	default:
		return nil, fmt.Errorf("unsupported DB scheme: %s", dbURL.Scheme)
	}

	return sql.Open(dbURL.Scheme, dsn)
}

func setupPermissionsTest(databaseURI string) error {
	db, err := openConnection(databaseURI)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE permissions_check (id integer)")
	if err != nil {
		return fmt.Errorf("Error creating table: %s", err.Error())
	}

	_, err = db.Exec("INSERT INTO permissions_check VALUES(42)")
	if err != nil {
		return fmt.Errorf("Error inserting record: %s", err.Error())
	}

	dbURL, err := url.Parse(databaseURI)
	if err != nil {
		return err
	}
	switch dbURL.Scheme {
	case "postgres":
		_, err = db.Exec("CREATE SCHEMA foo")
		if err != nil {
			return fmt.Errorf("Error creating a schema: %s", err.Error())
		}

		_, err = db.Exec("CREATE TABLE foo.bar (id integer)")
		if err != nil {
			return fmt.Errorf("Error creating a table within a schema: %s", err.Error())
		}

		_, err = db.Exec("INSERT INTO foo.bar (id) VALUES (1)")
		if err != nil {
			return fmt.Errorf("Error inserting into table within a schema: %s", err.Error())
		}
	case "mysql":
		// There are no MySQL-specific tests
	default:
		return fmt.Errorf("Scheme must either be postgres or mysql")
	}

	return nil
}

func permissionsTest(databaseURI string) error {
	db, err := openConnection(databaseURI)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO permissions_check VALUES (43)")
	if err != nil {
		return fmt.Errorf("Error inserting record: %s", err.Error())
	}

	_, err = db.Exec("ALTER TABLE permissions_check ADD COLUMN something INTEGER")
	if err != nil {
		return fmt.Errorf("Error ALTERing table: %s", err.Error())
	}

	_, err = db.Exec("DROP TABLE permissions_check")
	if err != nil {
		return fmt.Errorf("Error DROPing table: %s", err.Error())
	}

	dbURL, err := url.Parse(databaseURI)
	if err != nil {
		return err
	}
	switch dbURL.Scheme {
	case "postgres":
		_, err = db.Exec("DROP SCHEMA foo CASCADE")
		if err != nil {
			return fmt.Errorf("Error dropping schema: %s", err.Error())
		}
	case "mysql":
		// There are no MySQL-specific tests
	default:
		return fmt.Errorf("Scheme must either be postgres or mysql")
	}

	return nil
}

func postgresExtensionsTest(databaseURI string) error {
	db, err := openConnection(databaseURI)
	if err != nil {
		return err
	}
	defer db.Close()

	dbURL, err := url.Parse(databaseURI)
	if err != nil {
		return err
	}
	switch dbURL.Scheme {
	case "postgres":
		rows, err := db.Query("SELECT extname FROM pg_catalog.pg_extension")
		defer rows.Close()
		Expect(err).ToNot(HaveOccurred())
		extensions := []string{}
		for rows.Next() {
			var name string
			err = rows.Scan(&name)
			Expect(err).ToNot(HaveOccurred())
			extensions = append(extensions, name)
		}
		Expect(rows.Err()).ToNot(HaveOccurred())
		Expect(extensions).To(ContainElement("uuid-ossp"))
		Expect(extensions).To(ContainElement("postgis"))
	case "mysql":
		// There are no MySQL-specific tests
	default:
		return fmt.Errorf("Scheme must either be postgres or mysql")
	}

	return nil
}
