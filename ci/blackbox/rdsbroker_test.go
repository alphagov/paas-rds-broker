package integration_aws_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/phayes/freeport"

	uuid "github.com/satori/go.uuid"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
	"github.com/alphagov/paas-rds-broker/config"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

const (
	INSTANCE_CREATE_TIMEOUT = 30 * time.Minute
)

var _ = Describe("RDS Broker Daemon", func() {

	var (
		rdsBrokerSession *gexec.Session
		brokerAPIClient  *BrokerAPIClient
		rdsClient        *RDSClient
		brokerName       string
	)

	BeforeEach(func() {
		// Give a different Broker Name in each execution, to avoid conflicts
		brokerName = fmt.Sprintf(
			"%s-%s",
			"rdsbroker-integration-test",
			uuid.NewV4().String(),
		)

		rdsBrokerSession, brokerAPIClient, rdsClient = startNewBroker(suiteData.RdsBrokerConfig, brokerName)
	})

	AfterEach(func() {
		rdsBrokerSession.Kill()
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
			Expect(service2.Plans).To(HaveLen(4))
		})
	})

	Describe("Instance Provision/Bind/Deprovision and MasterPasswordSeed update", func() {

		TestProvisionBindDeprovision := func(serviceID, planID string) {
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
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))
			})

			AfterEach(func() {
				brokerAPIClient.AcceptsIncomplete = true
				code, operation, err := brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))
			})

			It("handles binding properly", func() {
				By("checking the instance credentials")
				Eventually(rdsBrokerSession, 30*time.Second).Should(gbytes.Say("credentials check has ended"))

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

				By("updating the backup and maintenance windows")
				code, operation, err := brokerAPIClient.UpdateInstance(instanceID, serviceID, planID, planID, `{"preferred_maintenance_window":"tue:10:00-tue:11:00", "preferred_backup_window":"21:00-22:00"}`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				details, err := rdsClient.GetDBInstanceDetails(instanceID)
				Expect(err).NotTo(HaveOccurred())
				Expect(details.DBInstances).To(HaveLen(1))
				Expect(aws.StringValue(details.DBInstances[0].PreferredMaintenanceWindow)).To(Equal("tue:10:00-tue:11:00"))
				Expect(aws.StringValue(details.DBInstances[0].PreferredBackupWindow)).To(Equal("21:00-22:00"))

				By("caching the instance details before master credentials rotation")
				detailsBefore := details

				By("restarting the broker with a new master password seed")
				rdsBrokerSession.Kill()
				newRDSConfig := *suiteData.RdsBrokerConfig.RDSConfig
				newRDSConfig.MasterPasswordSeed = "otherseed"
				newRDSBrokerConfig := *suiteData.RdsBrokerConfig
				newRDSBrokerConfig.RDSConfig = &newRDSConfig
				rdsBrokerSession, brokerAPIClient, rdsClient = startNewBroker(&newRDSBrokerConfig, brokerName)

				Eventually(rdsBrokerSession, 30*time.Second).Should(gbytes.Say("Will attempt to reset the password."))
				Eventually(rdsBrokerSession, 30*time.Second).Should(gbytes.Say("credentials check has ended"))

				By("immediately using the previous credentials to alter objects")
				err = setupPermissionsTest(credentials.URI)
				Expect(err).ToNot(HaveOccurred())

				By("re-binding shall eventually work")
				Eventually(
					func() bool {
						resp, err = brokerAPIClient.DoUnbindRequest(instanceID, serviceID, planID, bindingID)
						return (err == nil && resp.StatusCode == 200)
					},
					120*time.Second,
					15*time.Second,
				).Should(
					BeTrue(),
					"MasterPassword did not get updated",
				)
				resp, err = brokerAPIClient.DoBindRequest(instanceID, serviceID, planID, appGUID, bindingID)
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(201))

				By("using the new credentials to alter existing objects")
				credentials, err = getCredentialsFromBindResponse(resp)
				Expect(err).ToNot(HaveOccurred())
				err = permissionsTest(credentials.URI)
				Expect(err).ToNot(HaveOccurred())

				By("comparing current instance details with cache")
				detailsAfter, err := rdsClient.GetDBInstanceDetails(instanceID)
				Expect(err).NotTo(HaveOccurred())
				Expect(detailsAfter.DBInstances).To(HaveLen(1))
				// we expect certain values to change so set them to the same
				detailsBefore.DBInstances[0].LatestRestorableTime = detailsAfter.DBInstances[0].LatestRestorableTime
				detailsBefore.DBInstances[0].DBInstanceStatus = detailsAfter.DBInstances[0].DBInstanceStatus
				detailsBefore.DBInstances[0].PendingModifiedValues.MasterUserPassword = detailsAfter.DBInstances[0].PendingModifiedValues.MasterUserPassword
				Expect(detailsBefore.DBInstances[0]).To(Equal(detailsAfter.DBInstances[0]))
			})
		}

		Describe("Postgres 9.5", func() {
			TestProvisionBindDeprovision("postgres", "postgres-micro-without-snapshot")
		})

		Describe("Postgres 10.5", func() {
			TestProvisionBindDeprovision("postgres", "postgres-micro-without-snapshot-10")
		})

		Describe("MySQL", func() {
			TestProvisionBindDeprovision("mysql", "mysql-micro-without-snapshot")
		})
	})

	Describe("update extensions", func() {
		TestUpdateExtensions := func(serviceID, planID string) {
			var (
				instanceID string
			)

			BeforeEach(func() {
				instanceID = uuid.NewV4().String()

				brokerAPIClient.AcceptsIncomplete = true

				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))
			})

			AfterEach(func() {
				brokerAPIClient.AcceptsIncomplete = true
				code, operation, err := brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))
			})

			It("handles an enable/disable extensions", func() {
				By("enable extension")
				code, operation, err := brokerAPIClient.UpdateInstance(instanceID, serviceID, planID, planID, `{"enable_extensions": ["pg_stat_statements"], "reboot": true }`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				extensions := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(extensions).To(Equal("succeeded"))

				By("disable extension")
				code, operation, err = brokerAPIClient.UpdateInstance(instanceID, serviceID, planID, planID, `{"disable_extensions": ["pg_stat_statements"], "reboot": true }`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				extensions = pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(extensions).To(Equal("succeeded"))
			})
		}

		Describe("Postgres 9.5", func() {
			TestUpdateExtensions("postgres", "postgres-micro-without-snapshot")
		})

		Describe("Postgres 10.5", func() {
			TestUpdateExtensions("postgres", "postgres-micro-without-snapshot-10")
		})
	})

	Describe("Final snapshot enable/disable", func() {
		TestFinalSnapshot := func(serviceID, planID string) {
			var (
				instanceID      string
				finalSnapshotID string
			)

			BeforeEach(func() {
				instanceID = uuid.NewV4().String()
				finalSnapshotID = rdsClient.DBInstanceFinalSnapshotIdentifier(instanceID)
				brokerAPIClient.AcceptsIncomplete = true
			})

			It("should create a final snapshot by default", func() {
				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))

				snapshots, err := rdsClient.GetDBSnapshot(finalSnapshotID)
				fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshots)
				Expect(err).NotTo(HaveOccurred())
				Expect(snapshots).Should(ContainSubstring(instanceID))

				snapshotDeletionOutput, err := rdsClient.DeleteDBSnapshot(finalSnapshotID)
				fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should not create a final snapshot when `skip_final_snapshot` is set at provision time", func() {
				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, `{"skip_final_snapshot":true}`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))

				snapshots, err := rdsClient.GetDBSnapshot(finalSnapshotID)
				fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshots)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

				snapshotDeletionOutput, err := rdsClient.DeleteDBSnapshot(finalSnapshotID)
				fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
				Expect(err).To(HaveOccurred())
			})

			It("should not create a final snapshot when `skip_final_snapshot` is set via update", func() {
				code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.UpdateInstance(instanceID, serviceID, planID, planID, `{"skip_final_snapshot":true}`)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("succeeded"))

				code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
				Expect(err).ToNot(HaveOccurred())
				Expect(code).To(Equal(202))
				state = pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
				Expect(state).To(Equal("gone"))

				snapshots, err := rdsClient.GetDBSnapshot(finalSnapshotID)
				fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshots)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

				snapshotDeletionOutput, err := rdsClient.DeleteDBSnapshot(finalSnapshotID)
				fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
				fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
				Expect(err).To(HaveOccurred())
			})
		}

		Describe("Postgres 9.5", func() {
			TestFinalSnapshot("postgres", "postgres-micro")
		})

		Describe("Postgres 10.5", func() {
			TestFinalSnapshot("postgres", "postgres-micro-10")
		})

		Describe("MySQL", func() {
			TestFinalSnapshot("mysql", "mysql-micro")
		})
	})

	Describe("Restore from snapshot", func() {
		TestRestoreFromSnapshot := func(serviceID, planID string) {
			var (
				instanceID         string
				restoredInstanceID string
			)

			BeforeEach(func() {
				instanceID = uuid.NewV4().String()
				restoredInstanceID = uuid.NewV4().String()
				brokerAPIClient.AcceptsIncomplete = true
			})

			It("should create a final snapshot by default", func() {
				firstInstance := ProvisionManager{
					Provisioner: func() (WaitFunc, CleanFunc) {
						By("creating a first instance")
						code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, `{"skip_final_snapshot":true}`)
						Expect(err).ToNot(HaveOccurred())
						Expect(code).To(Equal(202))
						waiter := func() {
							state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
							Expect(state).To(Equal("succeeded"))
						}
						cleaner := func() {
							By("deleting the first instance")
							code, operation, err := brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
							Expect(err).ToNot(HaveOccurred())
							Expect(code).To(Equal(202))
							state := pollForOperationCompletion(brokerAPIClient, instanceID, serviceID, planID, operation)
							Expect(state).To(Equal("gone"))
						}
						return waiter, cleaner
					},
				}

				firstInstance.Provision()
				defer firstInstance.CleanUp()
				firstInstance.Wait()

				snapshot := ProvisionManager{
					Provisioner: func() (WaitFunc, CleanFunc) {
						By("creating a snapshot")
						snapshotID, err := rdsClient.CreateDBSnapshot(instanceID)
						Expect(err).ToNot(HaveOccurred())
						waiter := func() {
							Eventually(
								func() string {
									s, err := rdsClient.GetDBSnapshot(snapshotID)
									Expect(err).ToNot(HaveOccurred())
									return aws.StringValue(s.DBSnapshots[0].Status)
								},
								10*time.Minute,
								20*time.Second,
							).Should(
								Equal("available"),
							)
						}
						cleaner := func() {
							By("deleting the snapshot")
							snapshotDeletionOutput, err := rdsClient.DeleteDBSnapshot(snapshotID)
							fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
							fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
							Expect(err).ToNot(HaveOccurred())
						}
						return waiter, cleaner
					},
				}

				snapshot.Provision()
				defer snapshot.CleanUp()
				snapshot.Wait()

				firstInstance.CleanUp()

				secondInstance := ProvisionManager{
					Provisioner: func() (WaitFunc, CleanFunc) {
						By("restoring a second instance from snapshot")
						code, operation, err := brokerAPIClient.ProvisionInstance(
							restoredInstanceID, serviceID, planID,
							fmt.Sprintf(`{"skip_final_snapshot":true, "restore_from_latest_snapshot_of": "%s"}`, instanceID),
						)
						Expect(err).ToNot(HaveOccurred())
						Expect(code).To(Equal(202))
						waiter := func() {
							state := pollForOperationCompletion(brokerAPIClient, restoredInstanceID, serviceID, planID, operation)
							Expect(state).To(Equal("succeeded"))
						}
						cleaner := func() {
							By("deleting the second instance ")
							code, operation, err := brokerAPIClient.DeprovisionInstance(restoredInstanceID, serviceID, planID)
							Expect(err).ToNot(HaveOccurred())
							Expect(code).To(Equal(202))
							state := pollForOperationCompletion(brokerAPIClient, restoredInstanceID, serviceID, planID, operation)
							Expect(state).To(Equal("gone"))
						}
						return waiter, cleaner
					},
				}

				secondInstance.Provision()
				defer secondInstance.CleanUp()
				secondInstance.Wait()
			})
		}

		Describe("Postgres 9.5", func() {
			TestRestoreFromSnapshot("postgres", "postgres-micro")
		})

		Describe("Postgres 10.5", func() {
			TestRestoreFromSnapshot("postgres", "postgres-micro-10")
		})

		PDescribe("MySQL", func() {
			TestRestoreFromSnapshot("mysql", "mysql-micro")
		})
	})

})

func pollForOperationCompletion(brokerAPIClient *BrokerAPIClient, instanceID, serviceID, planID, operation string) string {
	var state string
	var err error

	fmt.Fprint(GinkgoWriter, "Polling for Instance Operation to complete")
	time.Sleep(15 * time.Second) // Ensure the operation has actually started in AWS
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

func startNewBroker(rdsBrokerConfig *config.Config, brokerName string) (*gexec.Session, *BrokerAPIClient, *RDSClient) {
	configFile, err := ioutil.TempFile("", "rds-broker")
	Expect(err).ToNot(HaveOccurred())
	defer os.Remove(configFile.Name())

	newRDSBrokerConfig := *rdsBrokerConfig
	// start the broker in a random port
	rdsBrokerPort := freeport.GetPort()
	newRDSBrokerConfig.Port = rdsBrokerPort

	newRDSBrokerConfig.RDSConfig.BrokerName = brokerName

	configJSON, err := json.Marshal(&newRDSBrokerConfig)
	Expect(err).ToNot(HaveOccurred())
	Expect(ioutil.WriteFile(configFile.Name(), configJSON, 0644)).To(Succeed())
	Expect(configFile.Close()).To(Succeed())

	command := exec.Command(suiteData.RdsBrokerPath,
		fmt.Sprintf("-config=%s", configFile.Name()),
	)
	rdsBrokerSession, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())

	// Wait for it to be listening
	Eventually(rdsBrokerSession, 10*time.Second).Should(And(
		gbytes.Say("rds-broker.start"),
		gbytes.Say(fmt.Sprintf(`{"port":%d}`, rdsBrokerPort)),
	))

	rdsBrokerUrl := fmt.Sprintf("http://localhost:%d", rdsBrokerPort)

	brokerAPIClient := NewBrokerAPIClient(rdsBrokerUrl, rdsBrokerConfig.Username, rdsBrokerConfig.Password)
	rdsClient, err := NewRDSClient(rdsBrokerConfig.RDSConfig.Region, rdsBrokerConfig.RDSConfig.DBPrefix)

	Expect(err).ToNot(HaveOccurred())

	return rdsBrokerSession, brokerAPIClient, rdsClient
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
		dsn = fmt.Sprintf("%s@tcp(%s)%s?tls=skip-verify",
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
