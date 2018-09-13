package rdsbroker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/rdsbroker"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/pivotal-cf/brokerapi"

	"github.com/alphagov/paas-rds-broker/awsrds"
	rdsfake "github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/alphagov/paas-rds-broker/sqlengine"
	sqlfake "github.com/alphagov/paas-rds-broker/sqlengine/fakes"
)

var _ = Describe("RDS Broker", func() {
	var (
		ctx context.Context

		rdsProperties1 RDSProperties
		rdsProperties2 RDSProperties
		rdsProperties3 RDSProperties
		plan1          ServicePlan
		plan2          ServicePlan
		plan3          ServicePlan
		service1       Service
		service2       Service
		service3       Service
		catalog        Catalog

		config Config

		dbInstance *rdsfake.FakeDBInstance

		sqlProvider *sqlfake.FakeProvider
		sqlEngine   *sqlfake.FakeSQLEngine

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsBroker *RDSBroker

		allowUserProvisionParameters bool
		allowUserUpdateParameters    bool
		allowUserBindParameters      bool
		serviceBindable              bool
		planUpdateable               bool
		skipFinalSnapshot            bool
		dbPrefix                     string
		brokerName                   string

		brokeruser      string
		brokerpass      string
		rdsBrokerServer http.Handler
	)

	const (
		masterPasswordSeed   = "something-secret"
		instanceID           = "instance-id"
		bindingID            = "binding-id"
		dbInstanceIdentifier = "cf-instance-id"
		dbName               = "cf_instance_id"
		dbUsername           = "uvMSB820K_t3WvCX"
		masterUserPassword   = "R2gfMWWb3naYDTL6rrBcGp-C5dmcThId"
		instanceOrigID       = "instance-orig-id"
	)

	BeforeEach(func() {
		ctx = context.Background()

		allowUserProvisionParameters = true
		allowUserUpdateParameters = true
		allowUserBindParameters = true
		serviceBindable = true
		planUpdateable = true
		skipFinalSnapshot = true
		dbPrefix = "cf"
		brokerName = "mybroker"

		dbInstance = &rdsfake.FakeDBInstance{}

		sqlProvider = &sqlfake.FakeProvider{}
		sqlEngine = &sqlfake.FakeSQLEngine{}
		sqlProvider.GetSQLEngineSQLEngine = sqlEngine

		rdsProperties1 = RDSProperties{
			DBInstanceClass:   "db.m1.test",
			Engine:            "test-engine-1",
			EngineVersion:     "1.2.3",
			AllocatedStorage:  100,
			SkipFinalSnapshot: skipFinalSnapshot,
		}

		rdsProperties2 = RDSProperties{
			DBInstanceClass:   "db.m2.test",
			Engine:            "test-engine-2",
			EngineVersion:     "4.5.6",
			AllocatedStorage:  200,
			SkipFinalSnapshot: skipFinalSnapshot,
		}

		rdsProperties3 = RDSProperties{
			DBInstanceClass:   "db.m3.test",
			Engine:            "postgres",
			EngineVersion:     "4.5.6",
			AllocatedStorage:  300,
			SkipFinalSnapshot: false,
		}
	})

	JustBeforeEach(func() {
		plan1 = ServicePlan{
			ID:            "Plan-1",
			Name:          "Plan 1",
			Description:   "This is the Plan 1",
			RDSProperties: rdsProperties1,
		}
		plan2 = ServicePlan{
			ID:            "Plan-2",
			Name:          "Plan 2",
			Description:   "This is the Plan 2",
			RDSProperties: rdsProperties2,
		}
		plan3 = ServicePlan{
			ID:            "Plan-3",
			Name:          "Plan 3",
			Description:   "This is the Plan 3",
			RDSProperties: rdsProperties3,
		}

		service1 = Service{
			ID:            "Service-1",
			Name:          "Service 1",
			Description:   "This is the Service 1",
			PlanUpdatable: planUpdateable,
			Plans:         []ServicePlan{plan1},
		}
		service2 = Service{
			ID:            "Service-2",
			Name:          "Service 2",
			Description:   "This is the Service 2",
			PlanUpdatable: planUpdateable,
			Plans:         []ServicePlan{plan2},
		}
		service3 = Service{
			ID:            "Service-3",
			Name:          "Service 3",
			Description:   "This is the Service 3",
			PlanUpdatable: planUpdateable,
			Plans:         []ServicePlan{plan3},
		}

		catalog = Catalog{
			Services: []Service{service1, service2, service3},
		}

		config = Config{
			Region:                       "rds-region",
			DBPrefix:                     dbPrefix,
			BrokerName:                   brokerName,
			MasterPasswordSeed:           masterPasswordSeed,
			AllowUserProvisionParameters: allowUserProvisionParameters,
			AllowUserUpdateParameters:    allowUserUpdateParameters,
			AllowUserBindParameters:      allowUserBindParameters,
			Catalog:                      catalog,
		}

		logger = lager.NewLogger("rdsbroker_test")
		gingkoSink := lager.NewWriterSink(GinkgoWriter, lager.INFO)
		logger.RegisterSink(gingkoSink)
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsBroker = New(config, dbInstance, sqlProvider, logger)

		brokeruser = "brokeruser"
		brokerpass = "brokerpass"

		credentials := brokerapi.BrokerCredentials{
			Username: "brokeruser",
			Password: "brokerpass",
		}

		rdsBrokerServer = brokerapi.New(rdsBroker, logger, credentials)
	})

	Describe("Services", func() {
		var (
			properCatalogResponse []brokerapi.Service
		)

		BeforeEach(func() {
			properCatalogResponse = []brokerapi.Service{
				brokerapi.Service{
					ID:            "Service-1",
					Name:          "Service 1",
					Description:   "This is the Service 1",
					Bindable:      serviceBindable,
					PlanUpdatable: planUpdateable,
					Plans: []brokerapi.ServicePlan{
						brokerapi.ServicePlan{
							ID:          "Plan-1",
							Name:        "Plan 1",
							Description: "This is the Plan 1",
						},
					},
				},
				brokerapi.Service{
					ID:            "Service-2",
					Name:          "Service 2",
					Description:   "This is the Service 2",
					Bindable:      serviceBindable,
					PlanUpdatable: planUpdateable,
					Plans: []brokerapi.ServicePlan{
						brokerapi.ServicePlan{
							ID:          "Plan-2",
							Name:        "Plan 2",
							Description: "This is the Plan 2",
						},
					},
				},
				brokerapi.Service{
					ID:            "Service-3",
					Name:          "Service 3",
					Description:   "This is the Service 3",
					Bindable:      serviceBindable,
					PlanUpdatable: planUpdateable,
					Plans: []brokerapi.ServicePlan{
						brokerapi.ServicePlan{
							ID:          "Plan-3",
							Name:        "Plan 3",
							Description: "This is the Plan 3",
						},
					},
				},
			}
		})

		It("returns the proper CatalogResponse", func() {
			brokerCatalog := rdsBroker.Services(ctx)
			Expect(brokerCatalog).To(Equal(properCatalogResponse))
		})

		It("brokerapi integration returns the proper CatalogResponse", func() {
			var err error

			recorder := httptest.NewRecorder()

			req, _ := http.NewRequest("GET", "http://example.com/v2/catalog", nil)
			req.SetBasicAuth(brokeruser, brokerpass)

			rdsBrokerServer.ServeHTTP(recorder, req)
			Expect(recorder.Code).To(Equal(200))

			catalog := brokerapi.CatalogResponse{}
			err = json.Unmarshal(recorder.Body.Bytes(), &catalog)
			Expect(err).ToNot(HaveOccurred())

			sort.Slice(
				catalog.Services,
				func(i, j int) bool {
					return catalog.Services[i].ID < catalog.Services[j].ID
				},
			)

			Expect(catalog.Services).To(HaveLen(3))
			service1 := catalog.Services[0]
			service2 := catalog.Services[1]
			service3 := catalog.Services[2]
			Expect(service1.ID).To(Equal("Service-1"))
			Expect(service2.ID).To(Equal("Service-2"))
			Expect(service3.ID).To(Equal("Service-3"))

			Expect(service1.ID).To(Equal("Service-1"))
			Expect(service1.Name).To(Equal("Service 1"))
			Expect(service1.Description).To(Equal("This is the Service 1"))
			Expect(service1.Bindable).To(BeTrue())
			Expect(service1.PlanUpdatable).To(BeTrue())
			Expect(service1.Plans).To(HaveLen(1))
			Expect(service1.Plans[0].ID).To(Equal("Plan-1"))
			Expect(service1.Plans[0].Name).To(Equal("Plan 1"))
			Expect(service1.Plans[0].Description).To(Equal("This is the Plan 1"))
		})

	})

	Describe("Provision", func() {
		var (
			provisionDetails  brokerapi.ProvisionDetails
			acceptsIncomplete bool

			properProvisionedServiceSpec brokerapi.ProvisionedServiceSpec
		)

		BeforeEach(func() {
			provisionDetails = brokerapi.ProvisionDetails{
				OrganizationGUID: "organization-id",
				PlanID:           "Plan-1",
				ServiceID:        "Service-1",
				SpaceGUID:        "space-id",
				RawParameters:    json.RawMessage{},
			}
			acceptsIncomplete = true

			properProvisionedServiceSpec = brokerapi.ProvisionedServiceSpec{
				IsAsync: true,
			}
		})

		Context("when custom parameters are not provided", func() {
			BeforeEach(func() {
				allowUserProvisionParameters = true
			})

			Context("when not present in request", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = nil
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when an empty JSON document", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage("{}")
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		It("returns the proper response", func() {
			provisionedServiceSpec, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
			Expect(provisionedServiceSpec).To(Equal(properProvisionedServiceSpec))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when restoring from a snapshot", func() {
			var (
				restoreFromSnapshotInstanceGUID string
				restoreFromSnapshotDBInstanceID string
				dbSnapshotTags                  map[string]string
			)

			BeforeEach(func() {
				rdsProperties1.Engine = "postgres"
				restoreFromSnapshotInstanceGUID = "guid-of-origin-instance"
				restoreFromSnapshotDBInstanceID = dbPrefix + "-" + restoreFromSnapshotInstanceGUID
				provisionDetails.RawParameters = json.RawMessage(`{"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `"}`)
				dbSnapshotTags = map[string]string{
					"Space ID":        "space-id",
					"Organization ID": "organization-id",
					"Plan ID":         "Plan-1",
				}
			})

			JustBeforeEach(func() {
				dbInstance.DescribeSnapshotsDBSnapshotsDetails = []*awsrds.DBSnapshotDetails{
					&awsrds.DBSnapshotDetails{
						Identifier:         restoreFromSnapshotDBInstanceID + "-1",
						InstanceIdentifier: restoreFromSnapshotDBInstanceID,
						CreateTime:         time.Now(),
						Tags:               dbSnapshotTags,
					},
					&awsrds.DBSnapshotDetails{
						Identifier:         restoreFromSnapshotDBInstanceID + "-2",
						InstanceIdentifier: restoreFromSnapshotDBInstanceID,
						CreateTime:         time.Now(),
						Tags:               dbSnapshotTags,
					},
				}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.DescribeSnapshotsCalled).To(BeTrue())
				Expect(dbInstance.DescribeSnapshotsDBInstanceID).To(Equal(restoreFromSnapshotDBInstanceID))
				Expect(dbInstance.RestoreCalled).To(BeTrue())
				Expect(dbInstance.RestoreID).To(Equal(dbInstanceIdentifier))
				Expect(dbInstance.RestoreDBInstanceDetails.DBInstanceClass).To(Equal("db.m1.test"))
				Expect(dbInstance.RestoreDBInstanceDetails.Engine).To(Equal("postgres"))
				Expect(dbInstance.RestoreDBInstanceDetails.DBName).To(BeEmpty())
				Expect(dbInstance.RestoreDBInstanceDetails.MasterUsername).To(BeEmpty())
				Expect(dbInstance.RestoreDBInstanceDetails.MasterUserPassword).To(BeEmpty())
				Expect(err).ToNot(HaveOccurred())
			})

			It("sets the right tags", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				expectedSnapshotIdentifier := dbInstance.DescribeSnapshotsDBSnapshotsDetails[0].Identifier
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Created by"]).To(Equal("AWS RDS Service Broker"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags).To(HaveKey("Created at"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-1"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-1"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Organization ID"]).To(Equal("organization-id"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Space ID"]).To(Equal("space-id"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["Restored From Snapshot"]).To(Equal(expectedSnapshotIdentifier))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["PendingResetUserPassword"]).To(Equal("true"))
				Expect(dbInstance.RestoreDBInstanceDetails.Tags["PendingUpdateSettings"]).To(Equal("true"))
				Expect(err).ToNot(HaveOccurred())
			})

			It("selects the latest snapshot", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				expectedSnapshotIdentifier := dbInstance.DescribeSnapshotsDBSnapshotsDetails[0].Identifier
				Expect(dbInstance.RestoreSnapshotIdentifier).To(Equal(expectedSnapshotIdentifier))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when the snapshot is in a different space", func() {
				BeforeEach(func() {
					dbSnapshotTags["Space ID"] = "different-space-id"
				})

				It("should fail to restore", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the snapshot is in a different org", func() {

				BeforeEach(func() {
					dbSnapshotTags["Organization ID"] = "different-organization-id"
				})

				It("should fail to restore", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("if it is using a different plan", func() {

				BeforeEach(func() {
					dbSnapshotTags["Plan ID"] = "different-plan-id"
				})

				It("should fail to restore", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when restoring the DB Instance fails", func() {
				BeforeEach(func() {
					dbInstance.RestoreError = errors.New("operation failed")
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("operation failed"))
				})
			})

			Context("and no snapshots are found", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeSnapshotsDBSnapshotsDetails = []*awsrds.DBSnapshotDetails{}
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("No snapshots found"))
				})
			})

			Context("when the engine is not 'postgres'", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = "some-other-engine"
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("not supported for engine"))
				})
			})

			Context("and the restore_from_latest_snapshot_of is an empty string", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"restore_from_latest_snapshot_of": ""}`)
				})
				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("Invalid guid"))
				})
			})

		})

		Context("when creating a new service instance", func() {
			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateCalled).To(BeTrue())
				Expect(dbInstance.CreateID).To(Equal(dbInstanceIdentifier))
				Expect(dbInstance.CreateDBInstanceDetails.DBInstanceClass).To(Equal("db.m1.test"))
				Expect(dbInstance.CreateDBInstanceDetails.Engine).To(Equal("test-engine-1"))
				Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal(dbName))
				Expect(dbInstance.CreateDBInstanceDetails.MasterUsername).ToNot(BeEmpty())
				Expect(dbInstance.CreateDBInstanceDetails.MasterUserPassword).To(Equal(masterUserPassword))
				Expect(err).ToNot(HaveOccurred())
			})

			It("sets the right tags", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Created by"]).To(Equal("AWS RDS Service Broker"))
				Expect(dbInstance.CreateDBInstanceDetails.Tags).To(HaveKey("Created at"))
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-1"))
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-1"))
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Organization ID"]).To(Equal("organization-id"))
				Expect(dbInstance.CreateDBInstanceDetails.Tags["Space ID"]).To(Equal("space-id"))
				Expect(err).ToNot(HaveOccurred())

			})

			It("does not set a 'Restored From Snapshot' tag", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.Tags).ToNot(HaveKey("Restored From Snapshot"))
				Expect(err).ToNot(HaveOccurred())
			})

			Context("creates a SkipFinalSnapshot tag", func() {

				Context("given there are no provision parameters set", func() {

					BeforeEach(func() {
						provisionDetails = brokerapi.ProvisionDetails{
							OrganizationGUID: "organization-id",
							PlanID:           "Plan-3",
							ServiceID:        "Service-3",
							SpaceGUID:        "space-id",
						}
					})

					It("sets the tag to false (default behaviour)", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.Tags["SkipFinalSnapshot"]).To(Equal("false"))
						Expect(err).ToNot(HaveOccurred())
					})
				})

				Context("given there are provision parameters set", func() {

					BeforeEach(func() {
						params := json.RawMessage(`{"SkipFinalSnapshot": "true"}`)
						provisionDetails.RawParameters = params
					})

					It("the parameters override the default", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.Tags["SkipFinalSnapshot"]).To(Equal("true"))
						Expect(err).ToNot(HaveOccurred())
					})
				})

			})

			Context("with a db prefix including - and _", func() {
				BeforeEach(func() {
					dbPrefix = "with-dash_underscore"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.CreateCalled).To(BeTrue())
					Expect(dbInstance.CreateID).To(Equal("with-dash-underscore-" + instanceID))
					expectedDBName := "with_dash_underscore_" + strings.Replace(instanceID, "-", "_", -1)
					Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal(expectedDBName))
				})
			})

			Context("when has AllocatedStorage", func() {
				BeforeEach(func() {
					rdsProperties1.AllocatedStorage = int64(100)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.AllocatedStorage).To(Equal(int64(100)))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has AutoMinorVersionUpgrade", func() {
				BeforeEach(func() {
					rdsProperties1.AutoMinorVersionUpgrade = true
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.AutoMinorVersionUpgrade).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has AvailabilityZone", func() {
				BeforeEach(func() {
					rdsProperties1.AvailabilityZone = "test-az"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has BackupRetentionPeriod", func() {
				BeforeEach(func() {
					rdsProperties1.BackupRetentionPeriod = int64(7)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(7)))
					Expect(err).ToNot(HaveOccurred())
				})

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has BackupRetentionPeriod Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"backup_retention_period": 12}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(12)))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})

			Context("when has default BackupRetentionPeriod", func() {
				It("has backups turned off", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(0)))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has CharacterSetName", func() {
				BeforeEach(func() {
					rdsProperties1.CharacterSetName = "test-characterset-name"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name"))
					Expect(err).ToNot(HaveOccurred())
				})

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has CharacterSetName Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"character_set_name": "test-characterset-name-parameter"}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})

			Context("when has CopyTagsToSnapshot", func() {
				BeforeEach(func() {
					rdsProperties1.CopyTagsToSnapshot = true
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.CopyTagsToSnapshot).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})
			})

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("when has DBName parameter", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"dbname": "test-dbname"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal("test-dbname"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has DBParameterGroupName", func() {
				BeforeEach(func() {
					rdsProperties1.DBParameterGroupName = "test-db-parameter-group-name"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.DBParameterGroupName).To(Equal("test-db-parameter-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has DBSecurityGroups", func() {
				BeforeEach(func() {
					rdsProperties1.DBSecurityGroups = []string{"test-db-security-group"}
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has DBSubnetGroupName", func() {
				BeforeEach(func() {
					rdsProperties1.DBSubnetGroupName = "test-db-subnet-group-name"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has EngineVersion", func() {
				BeforeEach(func() {
					rdsProperties1.EngineVersion = "1.2.3"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has Iops", func() {
				BeforeEach(func() {
					rdsProperties1.Iops = int64(1000)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.Iops).To(Equal(int64(1000)))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has KmsKeyID", func() {
				BeforeEach(func() {
					rdsProperties1.KmsKeyID = "test-kms-key-id"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.KmsKeyID).To(Equal("test-kms-key-id"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has LicenseModel", func() {
				BeforeEach(func() {
					rdsProperties1.LicenseModel = "test-license-model"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.LicenseModel).To(Equal("test-license-model"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has MultiAZ", func() {
				BeforeEach(func() {
					rdsProperties1.MultiAZ = true
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.MultiAZ).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has OptionGroupName", func() {
				BeforeEach(func() {
					rdsProperties1.OptionGroupName = "test-option-group-name"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.OptionGroupName).To(Equal("test-option-group-name"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has Port", func() {
				BeforeEach(func() {
					rdsProperties1.Port = int64(3306)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.Port).To(Equal(int64(3306)))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has PreferredBackupWindow", func() {
				BeforeEach(func() {
					rdsProperties1.PreferredBackupWindow = "test-preferred-backup-window"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
					Expect(err).ToNot(HaveOccurred())
				})

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has PreferredBackupWindow Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"preferred_backup_window": "test-preferred-backup-window-parameter"}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})

			Context("when has PreferredMaintenanceWindow", func() {
				BeforeEach(func() {
					rdsProperties1.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
					Expect(err).ToNot(HaveOccurred())
				})

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has PreferredMaintenanceWindow Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})

			Context("when has PubliclyAccessible", func() {
				BeforeEach(func() {
					rdsProperties1.PubliclyAccessible = true
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.PubliclyAccessible).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = true
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.StorageEncrypted).To(BeTrue())
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has StorageType", func() {
				BeforeEach(func() {
					rdsProperties1.StorageType = "test-storage-type"
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.StorageType).To(Equal("test-storage-type"))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when has VpcSecurityGroupIds", func() {
				BeforeEach(func() {
					rdsProperties1.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when request does not accept incomplete", func() {
				BeforeEach(func() {
					acceptsIncomplete = false
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
				})
			})
			Context("when Parameters are not valid", func() {

				Context("and user provision parameters are not allowed", func() {
					BeforeEach(func() {
						allowUserProvisionParameters = false
					})

					It("does not return an error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})

			Context("when Service Plan is not found", func() {
				BeforeEach(func() {
					provisionDetails.PlanID = "unknown"
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
				})
			})

			Context("when creating the DB Instance fails", func() {
				BeforeEach(func() {
					dbInstance.CreateError = errors.New("operation failed")
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("operation failed"))
				})
			})
		})

	})

	Describe("Update", func() {
		var (
			updateDetails           brokerapi.UpdateDetails
			acceptsIncomplete       bool
			properUpdateServiceSpec brokerapi.UpdateServiceSpec
		)

		BeforeEach(func() {
			updateDetails = brokerapi.UpdateDetails{
				ServiceID: "Service-2",
				PlanID:    "Plan-2",
				PreviousValues: brokerapi.PreviousValues{
					PlanID:    "Plan-1",
					ServiceID: "Service-1",
					OrgID:     "organization-id",
					SpaceID:   "space-id",
				},
				RawParameters: json.RawMessage{},
			}
			acceptsIncomplete = true
			properUpdateServiceSpec = brokerapi.UpdateServiceSpec{
				IsAsync: true,
			}
		})
		It("returns the proper response", func() {
			updateServiceSpec, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(updateServiceSpec).To(Equal(properUpdateServiceSpec))
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(dbInstance.ModifyCalled).To(BeTrue())
			Expect(dbInstance.ModifyID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.ModifyDBInstanceDetails.DBInstanceClass).To(Equal("db.m2.test"))
			Expect(dbInstance.ModifyDBInstanceDetails.Engine).To(Equal("test-engine-2"))
			Expect(err).ToNot(HaveOccurred())
		})

		It("sets the right tags", func() {
			_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Broker Name"]).To(Equal("mybroker"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Updated by"]).To(Equal("AWS RDS Service Broker"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags).To(HaveKey("Updated at"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-2"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-2"))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when custom parameters are not provided", func() {
			BeforeEach(func() {
				allowUserUpdateParameters = true
			})

			Context("when not present in request", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = nil
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when an empty JSON document", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage("{}")
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				rdsProperties2.AllocatedStorage = int64(100)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.AllocatedStorage).To(Equal(int64(100)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				rdsProperties2.AutoMinorVersionUpgrade = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.AutoMinorVersionUpgrade).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				rdsProperties2.AvailabilityZone = "test-az"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has ApplyAtMaintanceWindow", func() {

			var willApplyImmediately bool

			BeforeEach(func() {
				dbInstance.ModifyCallback = func(ID string, dbInstanceDetails awsrds.DBInstanceDetails, applyImmediately bool) error {
					willApplyImmediately = applyImmediately
					return nil
				}
			})

			It("applies immediately when apply_at_maintenance_window param is not given", func() {
				updateDetails.RawParameters = json.RawMessage(`{}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(willApplyImmediately).To(Equal(true))
			})

			It("applies immediately when apply_at_maintenance_window param is false", func() {
				updateDetails.RawParameters = json.RawMessage(`{"apply_at_maintenance_window": false}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(willApplyImmediately).To(Equal(true))
			})

			It("does not apply immediately when apply_at_maintenance_window param is true", func() {
				updateDetails.RawParameters = json.RawMessage(`{"apply_at_maintenance_window": true}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(willApplyImmediately).To(Equal(false))
			})

		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				rdsProperties2.BackupRetentionPeriod = int64(7)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(7)))
				Expect(err).ToNot(HaveOccurred())
			})

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("but has BackupRetentionPeriod Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`"backup_retention_period": 12}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(12)))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				rdsProperties2.CharacterSetName = "test-characterset-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				rdsProperties2.CopyTagsToSnapshot = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.CopyTagsToSnapshot).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBParameterGroupName = "test-db-parameter-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.DBParameterGroupName).To(Equal("test-db-parameter-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				rdsProperties2.DBSecurityGroups = []string{"test-db-security-group"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBSubnetGroupName = "test-db-subnet-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				rdsProperties2.EngineVersion = "1.2.3"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				rdsProperties2.Iops = int64(1000)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.Iops).To(Equal(int64(1000)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when storage encryption settings are updated", func() {
			Context("when tries to enable StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = false
					rdsProperties2.StorageEncrypted = true
				})

				It("fails noisily", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrEncryptionNotUpdateable))
				})
			})
			Context("when tries to disable StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = true
					rdsProperties2.StorageEncrypted = false
				})

				It("fails noisily", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrEncryptionNotUpdateable))
				})
			})
			Context("when changes KmsKeyID with StorageEncrypted enabled", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = true
					rdsProperties2.StorageEncrypted = true
					rdsProperties2.KmsKeyID = "test-old-kms-key-id"
					rdsProperties2.KmsKeyID = "test-new-kms-key-id"
				})

				It("fails noisily", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrEncryptionNotUpdateable))
				})
			})

		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				rdsProperties2.LicenseModel = "test-license-model"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.LicenseModel).To(Equal("test-license-model"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				rdsProperties2.MultiAZ = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.MultiAZ).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.OptionGroupName = "test-option-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.OptionGroupName).To(Equal("test-option-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				rdsProperties2.Port = int64(3306)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.Port).To(Equal(int64(3306)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredBackupWindow = "test-preferred-backup-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
				Expect(err).ToNot(HaveOccurred())
			})

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("but has PreferredBackupWindow Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`{"preferred_backup_window": "test-preferred-backup-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
				Expect(err).ToNot(HaveOccurred())
			})

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("but has PreferredMaintenanceWindow Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(dbInstance.ModifyDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				rdsProperties2.PubliclyAccessible = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.PubliclyAccessible).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				rdsProperties2.StorageType = "test-storage-type"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.StorageType).To(Equal("test-storage-type"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				rdsProperties2.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			Context("and user update parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserUpdateParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				updateDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Plans is not updateable", func() {
			BeforeEach(func() {
				planUpdateable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrPlanChangeNotSupported))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				updateDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when modifying the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.ModifyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.ModifyError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})
	})

	Describe("Deprovision", func() {
		var (
			deprovisionDetails           brokerapi.DeprovisionDetails
			acceptsIncomplete            bool
			properDeprovisionServiceSpec brokerapi.DeprovisionServiceSpec
		)

		BeforeEach(func() {
			deprovisionDetails = brokerapi.DeprovisionDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}
			acceptsIncomplete = true
			properDeprovisionServiceSpec = brokerapi.DeprovisionServiceSpec{
				IsAsync: true,
			}
		})

		It("returns the proper response", func() {
			deprovisionServiceSpec, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
			Expect(deprovisionServiceSpec).To(Equal(properDeprovisionServiceSpec))
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
			Expect(dbInstance.DeleteCalled).To(BeTrue())
			Expect(dbInstance.DeleteID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.DeleteSkipFinalSnapshot).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when it does not skip final snaphot", func() {
			BeforeEach(func() {
				rdsProperties1.SkipFinalSnapshot = false
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(dbInstance.DeleteCalled).To(BeTrue())
				Expect(dbInstance.DeleteID).To(Equal(dbInstanceIdentifier))
				Expect(dbInstance.DeleteSkipFinalSnapshot).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				deprovisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when deleting the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DeleteError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DeleteError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})
	})

	Describe("Bind", func() {
		var (
			bindDetails brokerapi.BindDetails
		)

		BeforeEach(func() {
			bindDetails = brokerapi.BindDetails{
				ServiceID:     "Service-1",
				PlanID:        "Plan-1",
				AppGUID:       "Application-1",
				RawParameters: json.RawMessage{},
			}

			dbInstance.DescribeDBInstanceDetails = awsrds.DBInstanceDetails{
				Identifier:     dbInstanceIdentifier,
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
			}

			sqlEngine.CreateUserUsername = dbUsername
			sqlEngine.CreateUserPassword = "secret"
		})

		It("returns the proper response", func() {
			bindingResponse, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
			Expect(err).ToNot(HaveOccurred())
			Expect(bindingResponse.Credentials).ToNot(BeNil())
			credentials := bindingResponse.Credentials.(Credentials)
			Expect(bindingResponse.SyslogDrainURL).To(BeEmpty())
			Expect(credentials.Host).To(Equal("endpoint-address"))
			Expect(credentials.Port).To(Equal(int64(3306)))
			Expect(credentials.Name).To(Equal("test-db"))
			Expect(credentials.Username).To(Equal(dbUsername))
			Expect(credentials.Password).To(Equal("secret"))
			Expect(credentials.URI).To(ContainSubstring("@endpoint-address:3306/test-db?reconnect=true"))
			Expect(credentials.JDBCURI).To(ContainSubstring("jdbc:fake://endpoint-address:3306/test-db?user=" + dbUsername + "&password="))
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
			Expect(dbInstance.DescribeCalled).To(BeTrue())
			Expect(dbInstance.DescribeID).To(Equal(dbInstanceIdentifier))
			Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
			Expect(sqlProvider.GetSQLEngineEngine).To(Equal("test-engine-1"))
			Expect(sqlEngine.OpenCalled).To(BeTrue())
			Expect(sqlEngine.OpenAddress).To(Equal("endpoint-address"))
			Expect(sqlEngine.OpenPort).To(Equal(int64(3306)))
			Expect(sqlEngine.OpenDBName).To(Equal("test-db"))
			Expect(sqlEngine.OpenUsername).To(Equal("master-username"))
			Expect(sqlEngine.OpenPassword).ToNot(BeEmpty())
			Expect(sqlEngine.CreateUserCalled).To(BeTrue())
			Expect(sqlEngine.CreateUserBindingID).To(Equal(bindingID))
			Expect(sqlEngine.CreateUserDBName).To(Equal("test-db"))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("brokerapi integration returns the proper response", func() {
			recorder := httptest.NewRecorder()

			bindingDetailsJson := []byte(`
				{
					"service_id": "Service-1",
					"plan_id": "Plan-1",
					"bind_resource": {
						"app_guid": "Application-1"
					},
					"parameters": {}
				}`)

			req, _ := http.NewRequest(
				"PUT",
				"http://example.com/v2/service_instances/"+
					instanceID+
					"/service_bindings/"+
					bindingID,
				bytes.NewBuffer(bindingDetailsJson),
			)
			req.SetBasicAuth(brokeruser, brokerpass)

			rdsBrokerServer.ServeHTTP(recorder, req)

			var bindingResponse struct {
				TheCredentials struct {
					TheHost     string `json:"host"`
					ThePort     int64  `json:"port"`
					TheName     string `json:"name"`
					TheUsername string `json:"username"`
					ThePassword string `json:"password"`
					TheURI      string `json:"uri"`
					TheJDBCURI  string `json:"jdbcuri"`
				} `json:"credentials"`
			}

			Expect(recorder.Body.String()).To(ContainSubstring(`"credentials"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"host"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"port"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"name"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"username"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"password"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"uri"`))
			Expect(recorder.Body.String()).To(ContainSubstring(`"jdbcuri"`))

			err := json.Unmarshal(recorder.Body.Bytes(), &bindingResponse)
			Expect(err).ToNot(HaveOccurred())
			fmt.Fprintf(GinkgoWriter, "%s:\n", recorder.Body.Bytes())
			fmt.Fprintf(GinkgoWriter, "%v:\n", bindingResponse)

			Expect(bindingResponse.TheCredentials.TheHost).To(Equal("endpoint-address"))
			Expect(bindingResponse.TheCredentials.ThePort).To(Equal(int64(3306)))
			Expect(bindingResponse.TheCredentials.TheName).To(Equal("test-db"))
			Expect(bindingResponse.TheCredentials.TheUsername).To(Equal(dbUsername))
			Expect(bindingResponse.TheCredentials.ThePassword).To(Equal("secret"))
			Expect(bindingResponse.TheCredentials.TheURI).To(ContainSubstring("@endpoint-address:3306/test-db?reconnect=true"))
			Expect(bindingResponse.TheCredentials.TheJDBCURI).To(ContainSubstring("jdbc:fake://endpoint-address:3306/test-db?user=" + dbUsername + "&password="))

			Expect(recorder.Code).To(Equal(201))

		})

		Context("when not using custom parameters", func() {
			BeforeEach(func() {
				allowUserBindParameters = true
			})

			Context("when absent from the request", func() {
				BeforeEach(func() {
					bindDetails.RawParameters = nil
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when present as an empty JSON document", func() {
				BeforeEach(func() {
					bindDetails.RawParameters = json.RawMessage("{}")
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		// FIXME: Re-enable these tests when we have some bind-time parameters again
		PContext("when Parameters are not valid", func() {
			BeforeEach(func() {
				bindDetails.RawParameters = json.RawMessage(`{"dbname": true}`)
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'dbname' expected type 'string', got unconvertible type 'bool'"))
			})

			Context("and user bind parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserBindParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				bindDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				bindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when getting the SQL Engine fails", func() {
			BeforeEach(func() {
				sqlProvider.GetSQLEngineError = errors.New("Engine 'unknown' not supported")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when creating a DB user fails", func() {
			BeforeEach(func() {
				sqlEngine.CreateUserError = errors.New("Failed to create user")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to create user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})
	})

	Describe("Unbind", func() {
		var (
			unbindDetails brokerapi.UnbindDetails
		)

		BeforeEach(func() {
			unbindDetails = brokerapi.UnbindDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}

			dbInstance.DescribeDBInstanceDetails = awsrds.DBInstanceDetails{
				Identifier:     dbInstanceIdentifier,
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
			}
		})

		It("makes the proper calls", func() {
			err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
			Expect(dbInstance.DescribeCalled).To(BeTrue())
			Expect(dbInstance.DescribeID).To(Equal(dbInstanceIdentifier))
			Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
			Expect(sqlProvider.GetSQLEngineEngine).To(Equal("test-engine-1"))
			Expect(sqlEngine.OpenCalled).To(BeTrue())
			Expect(sqlEngine.OpenAddress).To(Equal("endpoint-address"))
			Expect(sqlEngine.OpenPort).To(Equal(int64(3306)))
			Expect(sqlEngine.OpenDBName).To(Equal("test-db"))
			Expect(sqlEngine.OpenUsername).To(Equal("master-username"))
			Expect(sqlEngine.OpenPassword).ToNot(BeEmpty())
			Expect(sqlEngine.DropUserCalled).To(BeTrue())
			Expect(sqlEngine.DropUserBindingID).To(Equal(bindingID))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				unbindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when getting the SQL Engine fails", func() {
			BeforeEach(func() {
				sqlProvider.GetSQLEngineError = errors.New("SQL Engine 'unknown' not supported")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("SQL Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when deleting a user fails", func() {
			BeforeEach(func() {
				sqlEngine.DropUserError = errors.New("Failed to delete user")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to delete user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})
	})

	Describe("LastOperation", func() {
		var (
			dbInstanceStatus            string
			lastOperationState          brokerapi.LastOperationState
			properLastOperationResponse brokerapi.LastOperation
		)

		JustBeforeEach(func() {
			dbInstance.DescribeDBInstanceDetails = awsrds.DBInstanceDetails{
				Identifier:     dbInstanceIdentifier,
				Engine:         "test-engine",
				Address:        "endpoint-address",
				Port:           3306,
				DBName:         "test-db",
				MasterUsername: "master-username",
				Status:         dbInstanceStatus,
				Tags: map[string]string{
					"Owner":       "Cloud Foundry",
					"Broker Name": "mybroker",
					"Created by":  "AWS RDS Service Broker",
					"Service ID":  "Service-1",
					"Plan ID":     "Plan-1",
				},
			}

			properLastOperationResponse = brokerapi.LastOperation{
				State:       lastOperationState,
				Description: "DB Instance '" + dbInstanceIdentifier + "' status is '" + dbInstanceStatus + "'",
			}
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})

				It("brokerapi integration returns a status 410 Gone", func() {
					recorder := httptest.NewRecorder()

					req, _ := http.NewRequest(
						"GET",
						"http://example.com/v2/service_instances/"+instanceID+"/last_operation",
						nil,
					)
					req.SetBasicAuth(brokeruser, brokerpass)
					fmt.Fprintf(GinkgoWriter, "%s:\n", recorder.Body.Bytes())

					rdsBrokerServer.ServeHTTP(recorder, req)
					Expect(recorder.Code).To(Equal(410))
				})

			})
		})

		Context("when last operation is still in progress", func() {
			BeforeEach(func() {
				dbInstanceStatus = "creating"
				lastOperationState = brokerapi.InProgress
			})

			It("calls Describe() with the refresh cache option", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))

				Expect(dbInstance.DescribeOpts).To(ContainElement(awsrds.DescribeRefreshCacheOption))
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("and there are pending post restore tasks", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.Tags["PendingUpdateSettings"] = "true"
				})
				It("should not call RemoveTag to remove the tag PendingUpdateSettings", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.RemoveTagCalled).To(BeFalse())
				})

				It("should not modify the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.ModifyCalled).To(BeFalse())
				})
			})

		})

		Context("when last operation failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "failed"
				lastOperationState = brokerapi.Failed
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation succeeded", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				lastOperationState = brokerapi.Succeeded
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("the SQL engine is Postgres", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.Tags = map[string]string{
						"Plan ID": "Plan-3",
					}
				})

				It("attempts to create Postgres extenions", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.CreateExtensionsCalled).To(BeTrue())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})

			Context("but has pending modifications", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.PendingModifications = true

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending modifications",
					}
				})

				It("returns the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})

			Context("but there are pending post restore tasks", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.Tags["PendingUpdateSettings"] = "true"

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})
				It("should call RemoveTag to remove the tag PendingUpdateSettings", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.RemoveTagCalled).To(BeTrue())
					Expect(dbInstance.RemoveTagID).To(Equal(dbInstanceIdentifier))
					Expect(dbInstance.RemoveTagTagKey).To(Equal("PendingUpdateSettings"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						dbInstance.RemoveTagError = errors.New("Failed to remove tag")
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to remove tag"))
					})
				})

				It("modifies the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.ModifyCalled).To(BeTrue())
					Expect(dbInstance.ModifyID).To(Equal(dbInstanceIdentifier))
					Expect(err).ToNot(HaveOccurred())
				})

				It("sets the right tags", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(dbInstance.ModifyDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
					Expect(dbInstance.ModifyDBInstanceDetails.Tags["Broker Name"]).To(Equal("mybroker"))
					Expect(dbInstance.ModifyDBInstanceDetails.Tags["Restored by"]).To(Equal("AWS RDS Service Broker"))
					Expect(dbInstance.ModifyDBInstanceDetails.Tags).To(HaveKey("Restored at"))
					Expect(dbInstance.ModifyDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-1"))
					Expect(dbInstance.ModifyDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-1"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("when the master password needs to be rotated", func() {
					JustBeforeEach(func() {
						sqlEngine.CorrectPassword = "some-other-password"
						// use a callback function to set the password back to what it was before. This is because the Bind()
						// uses two different calls to the SQL engine's Open() method, the first needs to fail and the second needs to pass.
						dbInstance.ModifyCallback = func(ID string, dbInstanceDetails awsrds.DBInstanceDetails, applyImmediately bool) error {
							sqlEngine.CorrectPassword = dbInstanceDetails.MasterUserPassword
							return nil
						}
					})

					It("should try to change the master password", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(err).ToNot(HaveOccurred())
						Expect(dbInstance.ModifyCalled).To(BeTrue())
						Expect(dbInstance.ModifyID).To(BeEquivalentTo(dbInstanceIdentifier))
						Expect(dbInstance.ModifyDBInstanceDetails.MasterUserPassword).ToNot(BeEmpty())
					})
				})

				Context("when has DBSecurityGroups", func() {
					BeforeEach(func() {
						rdsProperties1.DBSecurityGroups = []string{"test-db-security-group"}
					})

					It("makes the modify with the secutiry group", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(dbInstance.ModifyDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})

			Context("but there are pending reboot", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.Tags["PendingReboot"] = "true"

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})

				It("should call RemoveTag to remove the tag PendingReboot", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.RemoveTagCalled).To(BeTrue())
					Expect(dbInstance.RemoveTagID).To(Equal(dbInstanceIdentifier))
					Expect(dbInstance.RemoveTagTagKey).To(Equal("PendingReboot"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						dbInstance.RemoveTagError = errors.New("Failed to remove tag")
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to remove tag"))
					})
				})

				It("reboot the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.RebootCalled).To(BeTrue())
					Expect(dbInstance.RebootID).To(Equal(dbInstanceIdentifier))
				})
			})

			Context("but there is a pending reset user password", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.Tags["PendingResetUserPassword"] = "true"

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})

				It("should call RemoveTag to remove the tag PendingResetUserPassword", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.RemoveTagCalled).To(BeTrue())
					Expect(dbInstance.RemoveTagID).To(Equal(dbInstanceIdentifier))
					Expect(dbInstance.RemoveTagTagKey).To(Equal("PendingResetUserPassword"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						dbInstance.RemoveTagError = errors.New("Failed to remove tag")
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to remove tag"))
					})
				})

				It("should reset the database state by calling sqlengine.ResetState()", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.ResetStateCalled).To(BeTrue())
				})

				Context("when sqlengine.ResetState() fails", func() {
					BeforeEach(func() {
						sqlEngine.ResetStateError = errors.New("Failed to reset state")
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to reset state"))
					})
				})
			})

			Context("but there are not post restore tasks or reset password to execute", func() {
				It("should not try to change the master password", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.ModifyCalled).To(BeFalse())
				})
				It("should not reset the database state by not calling sqlengine.ResetState()", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.ResetStateCalled).To(BeFalse())
				})
				It("should not call RemoveTag", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(dbInstance.RemoveTagCalled).To(BeFalse())
				})
			})
		})

		checkLastOperationResponse := func(instanceStatus string, expectedLastOperationState brokerapi.LastOperationState) func() {
			return func() {
				BeforeEach(func() {
					dbInstanceStatus = instanceStatus
					lastOperationState = expectedLastOperationState
				})

				It("returns the state "+string(expectedLastOperationState), func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			}
		}

		failureStatuses := []string{
			"failed",
			"inaccessible-encryption-credentials",
			"incompatible-credentials",
			"incompatible-network",
			"incompatible-option-group",
			"incompatible-parameters",
			"incompatible-restore",
			"restore-error",
		}
		for _, instanceStatus := range failureStatuses {
			Context("when instance status is "+instanceStatus, checkLastOperationResponse(instanceStatus, brokerapi.Failed))
		}

		successStatuses := []string{
			"available",
		}
		for _, instanceStatus := range successStatuses {
			Context("when instance status is "+instanceStatus, checkLastOperationResponse(instanceStatus, brokerapi.Succeeded))
		}

		inProgressStatuses := []string{
			"backing-up",
			"configuring-enhanced-monitoring",
			"creating",
			"deleting",
			"maintenance",
			"modifying",
			"rebooting",
			"renaming",
			"resetting-master-credentials",
			"starting",
			"stopping",
			"stopped",
			"storage-full",
			"storage-optimization",
			"upgrading",
		}
		for _, instanceStatus := range inProgressStatuses {
			Context("when instance status is "+instanceStatus, checkLastOperationResponse(instanceStatus, brokerapi.InProgress))
		}

		unexpectedStatuses := []string{
			"",
			"some-new-status",
		}
		for _, instanceStatus := range unexpectedStatuses {
			Context("when instance status is "+instanceStatus, checkLastOperationResponse(instanceStatus, brokerapi.InProgress))
		}

	})

	Describe("CheckAndRotateCredentials", func() {
		Context("when there is no DB instance", func() {
			It("shouldn't try to connect to databases", func() {
				rdsBroker.CheckAndRotateCredentials()
				Expect(sqlProvider.GetSQLEngineCalled).To(BeFalse())
				Expect(sqlEngine.OpenCalled).To(BeFalse())
			})
		})

		Context("when there are DB instances", func() {
			BeforeEach(func() {
				dbInstance.DescribeByTagDBInstanceDetails = []*awsrds.DBInstanceDetails{
					&awsrds.DBInstanceDetails{
						Identifier:     dbInstanceIdentifier,
						Address:        "endpoint-address",
						Port:           3306,
						DBName:         "test-db",
						MasterUsername: "master-username",
						Engine:         "fake-engine",
					},
				}
			})

			It("should try to connect to databases", func() {
				rdsBroker.CheckAndRotateCredentials()
				Expect(dbInstance.DescribeByTagCalled).To(BeTrue())
				Expect(dbInstance.DescribeByTagKey).To(BeEquivalentTo("Broker Name"))
				Expect(dbInstance.DescribeByTagValue).To(BeEquivalentTo(brokerName))
				Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
				Expect(sqlProvider.GetSQLEngineEngine).To(BeEquivalentTo("fake-engine"))
				Expect(sqlEngine.OpenCalled).To(BeTrue())
				Expect(sqlEngine.OpenAddress).To(BeEquivalentTo("endpoint-address"))
				Expect(sqlEngine.OpenPort).To(BeEquivalentTo(3306))
				Expect(sqlEngine.OpenDBName).To(BeEquivalentTo("test-db"))
				Expect(sqlEngine.OpenUsername).To(BeEquivalentTo("master-username"))
			})

			Context("and the passwords work", func() {
				It("should not try to change the master password", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(dbInstance.ModifyCalled).To(BeFalse())
				})
			})

			Context("and the passwords don't work", func() {
				BeforeEach(func() {
					sqlEngine.OpenError = sqlengine.LoginFailedError
				})

				It("should try to change the master password", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(dbInstance.ModifyCalled).To(BeTrue())
					Expect(dbInstance.ModifyID).To(BeEquivalentTo(dbInstanceIdentifier))
					Expect(dbInstance.ModifyDBInstanceDetails.MasterUserPassword).To(BeEquivalentTo(sqlEngine.OpenPassword))
				})
			})

			Context("and there is an unkown open error", func() {
				BeforeEach(func() {
					sqlEngine.OpenError = errors.New("Unknown open connection error")
				})

				It("should not try to change the master password", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(dbInstance.ModifyCalled).To(BeFalse())
				})
			})

			Context("and there is DescribeByTagError error", func() {
				BeforeEach(func() {
					dbInstance.DescribeByTagError = errors.New("Error when listing instances")
				})

				It("should exit with an error", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(dbInstance.ModifyCalled).To(BeFalse())
				})
			})

		})

		Context("when we reset the password then try to bind", func() {
			var (
				bindDetails brokerapi.BindDetails
			)

			BeforeEach(func() {
				dbInstance.DescribeByTagDBInstanceDetails = []*awsrds.DBInstanceDetails{
					&awsrds.DBInstanceDetails{
						Identifier:     dbInstanceIdentifier,
						Address:        "endpoint-address",
						Port:           3306,
						DBName:         "test-db",
						MasterUsername: "master-username",
						Engine:         "fake-engine",
					},
				}

				bindDetails = brokerapi.BindDetails{
					ServiceID:     "Service-1",
					PlanID:        "Plan-1",
					AppGUID:       "Application-1",
					RawParameters: json.RawMessage("{}"),
				}
			})

			It("the new password and the password used in bind are the same", func() {
				sqlEngine.OpenError = sqlengine.LoginFailedError
				rdsBroker.CheckAndRotateCredentials()
				expectedMasterPassword := sqlEngine.OpenPassword
				Expect(dbInstance.ModifyCalled).To(BeTrue())
				Expect(dbInstance.ModifyDBInstanceDetails.MasterUserPassword).To(BeEquivalentTo(expectedMasterPassword))

				sqlEngine.OpenError = nil
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(sqlEngine.OpenCalled).To(BeTrue())

				Expect(sqlEngine.OpenPassword).To(BeEquivalentTo(expectedMasterPassword))
			})
		})
	})

})
