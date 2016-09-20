package rdsbroker_test

import (
	"errors"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/rdsbroker"

	"github.com/frodenas/brokerapi"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"

	"github.com/alphagov/paas-rds-broker/awsrds"
	rdsfake "github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/alphagov/paas-rds-broker/sqlengine"
	sqlfake "github.com/alphagov/paas-rds-broker/sqlengine/fakes"
)

var _ = Describe("RDS Broker", func() {
	var (
		rdsProperties1 RDSProperties
		rdsProperties2 RDSProperties
		plan1          ServicePlan
		plan2          ServicePlan
		service1       Service
		service2       Service
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
	)

	const (
		masterPasswordSeed   = "something-secret"
		instanceID           = "instance-id"
		bindingID            = "binding-id"
		dbInstanceIdentifier = "cf-instance-id"
		dbName               = "cf_instance_id"
		dbUsername           = "uvMSB820K_t3WvCX"
		masterUserPassword   = "qOeiJ6AstR_mUQJxn6jyew=="
	)

	BeforeEach(func() {
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

		service1 = Service{
			ID:             "Service-1",
			Name:           "Service 1",
			Description:    "This is the Service 1",
			Bindable:       serviceBindable,
			PlanUpdateable: planUpdateable,
			Plans:          []ServicePlan{plan1},
		}
		service2 = Service{
			ID:             "Service-2",
			Name:           "Service 2",
			Description:    "This is the Service 2",
			Bindable:       serviceBindable,
			PlanUpdateable: planUpdateable,
			Plans:          []ServicePlan{plan2},
		}

		catalog = Catalog{
			Services: []Service{service1, service2},
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
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsBroker = New(config, dbInstance, sqlProvider, logger)
	})

	var _ = Describe("Services", func() {
		var (
			properCatalogResponse brokerapi.CatalogResponse
		)

		BeforeEach(func() {
			properCatalogResponse = brokerapi.CatalogResponse{
				Services: []brokerapi.Service{
					brokerapi.Service{
						ID:             "Service-1",
						Name:           "Service 1",
						Description:    "This is the Service 1",
						Bindable:       serviceBindable,
						PlanUpdateable: planUpdateable,
						Plans: []brokerapi.ServicePlan{
							brokerapi.ServicePlan{
								ID:          "Plan-1",
								Name:        "Plan 1",
								Description: "This is the Plan 1",
							},
						},
					},
					brokerapi.Service{
						ID:             "Service-2",
						Name:           "Service 2",
						Description:    "This is the Service 2",
						Bindable:       serviceBindable,
						PlanUpdateable: planUpdateable,
						Plans: []brokerapi.ServicePlan{
							brokerapi.ServicePlan{
								ID:          "Plan-2",
								Name:        "Plan 2",
								Description: "This is the Plan 2",
							},
						},
					},
				},
			}
		})

		It("returns the proper CatalogResponse", func() {
			brokerCatalog := rdsBroker.Services()
			Expect(brokerCatalog).To(Equal(properCatalogResponse))
		})

	})

	var _ = Describe("Provision", func() {
		var (
			provisionDetails  brokerapi.ProvisionDetails
			acceptsIncomplete bool

			properProvisioningResponse brokerapi.ProvisioningResponse
		)

		BeforeEach(func() {
			provisionDetails = brokerapi.ProvisionDetails{
				OrganizationGUID: "organization-id",
				PlanID:           "Plan-1",
				ServiceID:        "Service-1",
				SpaceGUID:        "space-id",
				Parameters:       map[string]interface{}{},
			}
			acceptsIncomplete = true

			properProvisioningResponse = brokerapi.ProvisioningResponse{}
		})

		It("returns the proper response", func() {
			provisioningResponse, asynch, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
			Expect(provisioningResponse).To(Equal(properProvisioningResponse))
			Expect(asynch).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
			Expect(dbInstance.CreateCalled).To(BeTrue())
			Expect(dbInstance.CreateID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.CreateDBInstanceDetails.DBInstanceClass).To(Equal("db.m1.test"))
			Expect(dbInstance.CreateDBInstanceDetails.Engine).To(Equal("test-engine-1"))
			Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal(dbName))
			Expect(dbInstance.CreateDBInstanceDetails.MasterUsername).ToNot(BeEmpty())
			Expect(dbInstance.CreateDBInstanceDetails.MasterUserPassword).To(Equal(masterUserPassword))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Created by"]).To(Equal("AWS RDS Service Broker"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags).To(HaveKey("Created at"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-1"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-1"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Organization ID"]).To(Equal("organization-id"))
			Expect(dbInstance.CreateDBInstanceDetails.Tags["Space ID"]).To(Equal("space-id"))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("with a db prefix including - and _", func() {
			BeforeEach(func() {
				dbPrefix = "with-dash_underscore"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
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
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.AllocatedStorage).To(Equal(int64(100)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				rdsProperties1.AutoMinorVersionUpgrade = true
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.AutoMinorVersionUpgrade).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				rdsProperties1.AvailabilityZone = "test-az"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				rdsProperties1.BackupRetentionPeriod = int64(7)
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(7)))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has BackupRetentionPeriod Parameter", func() {
					BeforeEach(func() {
						provisionDetails.Parameters = map[string]interface{}{"backup_retention_period": 12}
					})

					It("makes the proper calls", func() {
						_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(12)))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has default BackupRetentionPeriod", func() {
			It("has backups turned off", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(0)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				rdsProperties1.CharacterSetName = "test-characterset-name"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name"))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has CharacterSetName Parameter", func() {
					BeforeEach(func() {
						provisionDetails.Parameters = map[string]interface{}{"character_set_name": "test-characterset-name-parameter"}
					})

					It("makes the proper calls", func() {
						_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				rdsProperties1.CopyTagsToSnapshot = true
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.CopyTagsToSnapshot).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})
		/*
			Context("when has DBName parameter", func() {
				BeforeEach(func() {
					provisionDetails.Parameters = map[string]interface{}{"dbname": "test-dbname"}
				})

				It("makes the proper calls", func() {
					_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
					Expect(dbInstance.CreateDBInstanceDetails.DBName).To(Equal("test-dbname"))
					Expect(err).ToNot(HaveOccurred())
				})
			})
		*/
		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				rdsProperties1.DBParameterGroupName = "test-db-parameter-group-name"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.DBParameterGroupName).To(Equal("test-db-parameter-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				rdsProperties1.DBSecurityGroups = []string{"test-db-security-group"}
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				rdsProperties1.DBSubnetGroupName = "test-db-subnet-group-name"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				rdsProperties1.EngineVersion = "1.2.3"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				rdsProperties1.Iops = int64(1000)
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.Iops).To(Equal(int64(1000)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has KmsKeyID", func() {
			BeforeEach(func() {
				rdsProperties1.KmsKeyID = "test-kms-key-id"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.KmsKeyID).To(Equal("test-kms-key-id"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				rdsProperties1.LicenseModel = "test-license-model"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.LicenseModel).To(Equal("test-license-model"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				rdsProperties1.MultiAZ = true
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.MultiAZ).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				rdsProperties1.OptionGroupName = "test-option-group-name"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.OptionGroupName).To(Equal("test-option-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				rdsProperties1.Port = int64(3306)
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.Port).To(Equal(int64(3306)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				rdsProperties1.PreferredBackupWindow = "test-preferred-backup-window"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has PreferredBackupWindow Parameter", func() {
					BeforeEach(func() {
						provisionDetails.Parameters = map[string]interface{}{"preferred_backup_window": "test-preferred-backup-window-parameter"}
					})

					It("makes the proper calls", func() {
						_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				rdsProperties1.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has PreferredMaintenanceWindow Parameter", func() {
					BeforeEach(func() {
						provisionDetails.Parameters = map[string]interface{}{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}
					})

					It("makes the proper calls", func() {
						_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
						Expect(dbInstance.CreateDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				rdsProperties1.PubliclyAccessible = true
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.PubliclyAccessible).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageEncrypted", func() {
			BeforeEach(func() {
				rdsProperties1.StorageEncrypted = true
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.StorageEncrypted).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				rdsProperties1.StorageType = "test-storage-type"
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.StorageType).To(Equal("test-storage-type"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				rdsProperties1.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
			})

			It("makes the proper calls", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(dbInstance.CreateDBInstanceDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})
		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				provisionDetails.Parameters = map[string]interface{}{"skip_final_snapshot": "invalid"}
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'skip_final_snapshot' expected type 'bool', got unconvertible type 'string'"))
			})

			Context("and user provision parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserProvisionParameters = false
				})

				It("does not return an error", func() {
					_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				provisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when creating the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.CreateError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, _, err := rdsBroker.Provision(instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})
	})

	var _ = Describe("Update", func() {
		var (
			updateDetails     brokerapi.UpdateDetails
			acceptsIncomplete bool
		)

		BeforeEach(func() {
			updateDetails = brokerapi.UpdateDetails{
				ServiceID:  "Service-2",
				PlanID:     "Plan-2",
				Parameters: map[string]interface{}{},
				PreviousValues: brokerapi.PreviousValues{
					PlanID:         "Plan-1",
					ServiceID:      "Service-1",
					OrganizationID: "organization-id",
					SpaceID:        "space-id",
				},
			}
			acceptsIncomplete = true
		})

		It("returns the proper response", func() {
			asynch, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
			Expect(asynch).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
			Expect(dbInstance.ModifyCalled).To(BeTrue())
			Expect(dbInstance.ModifyID).To(Equal(dbInstanceIdentifier))
			Expect(dbInstance.ModifyDBInstanceDetails.DBInstanceClass).To(Equal("db.m2.test"))
			Expect(dbInstance.ModifyDBInstanceDetails.Engine).To(Equal("test-engine-2"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Owner"]).To(Equal("Cloud Foundry"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Broker Name"]).To(Equal("mybroker"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Updated by"]).To(Equal("AWS RDS Service Broker"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags).To(HaveKey("Updated at"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Service ID"]).To(Equal("Service-2"))
			Expect(dbInstance.ModifyDBInstanceDetails.Tags["Plan ID"]).To(Equal("Plan-2"))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				rdsProperties2.AllocatedStorage = int64(100)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.AllocatedStorage).To(Equal(int64(100)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				rdsProperties2.AutoMinorVersionUpgrade = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.AutoMinorVersionUpgrade).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				rdsProperties2.AvailabilityZone = "test-az"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.AvailabilityZone).To(Equal("test-az"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				rdsProperties2.BackupRetentionPeriod = int64(7)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(7)))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has BackupRetentionPeriod Parameter", func() {
					BeforeEach(func() {
						updateDetails.Parameters = map[string]interface{}{"backup_retention_period": 12}
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
						Expect(dbInstance.ModifyDBInstanceDetails.BackupRetentionPeriod).To(Equal(int64(12)))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				rdsProperties2.CharacterSetName = "test-characterset-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.CharacterSetName).To(Equal("test-characterset-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				rdsProperties2.CopyTagsToSnapshot = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.CopyTagsToSnapshot).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBParameterGroupName = "test-db-parameter-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.DBParameterGroupName).To(Equal("test-db-parameter-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				rdsProperties2.DBSecurityGroups = []string{"test-db-security-group"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.DBSecurityGroups).To(Equal([]string{"test-db-security-group"}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBSubnetGroupName = "test-db-subnet-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.DBSubnetGroupName).To(Equal("test-db-subnet-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				rdsProperties2.EngineVersion = "1.2.3"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.EngineVersion).To(Equal("1.2.3"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				rdsProperties2.Iops = int64(1000)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.Iops).To(Equal(int64(1000)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has KmsKeyID", func() {
			BeforeEach(func() {
				rdsProperties2.KmsKeyID = "test-kms-key-id"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.KmsKeyID).To(Equal("test-kms-key-id"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				rdsProperties2.LicenseModel = "test-license-model"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.LicenseModel).To(Equal("test-license-model"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				rdsProperties2.MultiAZ = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.MultiAZ).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.OptionGroupName = "test-option-group-name"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.OptionGroupName).To(Equal("test-option-group-name"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				rdsProperties2.Port = int64(3306)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.Port).To(Equal(int64(3306)))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredBackupWindow = "test-preferred-backup-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window"))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has PreferredBackupWindow Parameter", func() {
					BeforeEach(func() {
						updateDetails.Parameters = map[string]interface{}{"preferred_backup_window": "test-preferred-backup-window-parameter"}
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
						Expect(dbInstance.ModifyDBInstanceDetails.PreferredBackupWindow).To(Equal("test-preferred-backup-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window"))
				Expect(err).ToNot(HaveOccurred())
			})
			/*
				Context("but has PreferredMaintenanceWindow Parameter", func() {
					BeforeEach(func() {
						updateDetails.Parameters = map[string]interface{}{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
						Expect(dbInstance.ModifyDBInstanceDetails.PreferredMaintenanceWindow).To(Equal("test-preferred-maintenance-window-parameter"))
						Expect(err).ToNot(HaveOccurred())
					})
				})
			*/
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				rdsProperties2.PubliclyAccessible = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.PubliclyAccessible).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageEncrypted", func() {
			BeforeEach(func() {
				rdsProperties2.StorageEncrypted = true
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.StorageEncrypted).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				rdsProperties2.StorageType = "test-storage-type"
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.StorageType).To(Equal("test-storage-type"))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				rdsProperties2.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(dbInstance.ModifyDBInstanceDetails.VpcSecurityGroupIds).To(Equal([]string{"test-vpc-security-group-ids"}))
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			BeforeEach(func() {
				updateDetails.Parameters = map[string]interface{}{"skip_final_snapshot": "invalid"}
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'skip_final_snapshot' expected type 'bool', got unconvertible type 'string'"))
			})

			Context("and user update parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserUpdateParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				updateDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Plans is not updateable", func() {
			BeforeEach(func() {
				planUpdateable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceNotUpdateable))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				updateDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when modifying the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.ModifyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.ModifyError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Deprovision", func() {
		var (
			deprovisionDetails brokerapi.DeprovisionDetails
			acceptsIncomplete  bool
		)

		BeforeEach(func() {
			deprovisionDetails = brokerapi.DeprovisionDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
			}
			acceptsIncomplete = true
		})

		It("returns the proper response", func() {
			asynch, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
			Expect(asynch).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
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
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
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
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				deprovisionDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when deleting the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DeleteError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DeleteError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Deprovision(instanceID, deprovisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Bind", func() {
		var (
			bindDetails brokerapi.BindDetails
		)

		BeforeEach(func() {
			bindDetails = brokerapi.BindDetails{
				ServiceID:  "Service-1",
				PlanID:     "Plan-1",
				AppGUID:    "Application-1",
				Parameters: map[string]interface{}{},
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
			bindingResponse, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
			credentials := bindingResponse.Credentials.(*brokerapi.CredentialsHash)
			Expect(bindingResponse.SyslogDrainURL).To(BeEmpty())
			Expect(credentials.Host).To(Equal("endpoint-address"))
			Expect(credentials.Port).To(Equal(int64(3306)))
			Expect(credentials.Name).To(Equal("test-db"))
			Expect(credentials.Username).To(Equal(dbUsername))
			Expect(credentials.Password).To(Equal("secret"))
			Expect(credentials.URI).To(ContainSubstring("@endpoint-address:3306/test-db?reconnect=true"))
			Expect(credentials.JDBCURI).To(ContainSubstring("jdbc:fake://endpoint-address:3306/test-db?user=" + dbUsername + "&password="))
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
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

		// FIXME: Re-enable these tests when we have some bind-time parameters again
		PContext("when Parameters are not valid", func() {
			BeforeEach(func() {
				bindDetails.Parameters = map[string]interface{}{"dbname": true}
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("'dbname' expected type 'string', got unconvertible type 'bool'"))
			})

			Context("and user bind parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserBindParameters = false
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				bindDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Service is not bindable", func() {
			BeforeEach(func() {
				serviceBindable = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(brokerapi.ErrInstanceNotBindable))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				bindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
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
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when creating a DB user fails", func() {
			BeforeEach(func() {
				sqlEngine.CreateUserError = errors.New("Failed to create user")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to create user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})
	})

	var _ = Describe("Unbind", func() {
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
			err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
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
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
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
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("SQL Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when deleting a user fails", func() {
			BeforeEach(func() {
				sqlEngine.DropUserError = errors.New("Failed to delete user")
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to delete user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
			})
		})
	})

	var _ = Describe("LastOperation", func() {
		var (
			dbInstanceStatus            string
			lastOperationState          string
			properLastOperationResponse brokerapi.LastOperationResponse
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
			}

			properLastOperationResponse = brokerapi.LastOperationResponse{
				State:       lastOperationState,
				Description: "DB Instance '" + dbInstanceIdentifier + "' status is '" + dbInstanceStatus + "'",
			}
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				dbInstance.DescribeError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.LastOperation(instanceID)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					dbInstance.DescribeError = awsrds.ErrDBInstanceDoesNotExist
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.LastOperation(instanceID)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when last operation is still in progress", func() {
			BeforeEach(func() {
				dbInstanceStatus = "creating"
				lastOperationState = brokerapi.LastOperationInProgress
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "failed"
				lastOperationState = brokerapi.LastOperationFailed
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when last operation succeeded", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				lastOperationState = brokerapi.LastOperationSucceeded
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("but has pending modifications", func() {
				JustBeforeEach(func() {
					dbInstance.DescribeDBInstanceDetails.PendingModifications = true

					properLastOperationResponse = brokerapi.LastOperationResponse{
						State:       brokerapi.LastOperationInProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending modifications",
					}
				})

				It("returns the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(instanceID)
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})
		})
	})

	var _ = Describe("CheckAndRotateCredentials", func() {
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
					ServiceID:  "Service-1",
					PlanID:     "Plan-1",
					AppGUID:    "Application-1",
					Parameters: map[string]interface{}{},
				}
			})

			It("the new password and the password used in bind are the same", func() {
				sqlEngine.OpenError = sqlengine.LoginFailedError
				rdsBroker.CheckAndRotateCredentials()
				expectedMasterPassword := sqlEngine.OpenPassword
				Expect(dbInstance.ModifyCalled).To(BeTrue())
				Expect(dbInstance.ModifyDBInstanceDetails.MasterUserPassword).To(BeEquivalentTo(expectedMasterPassword))

				sqlEngine.OpenError = nil
				_, err := rdsBroker.Bind(instanceID, bindingID, bindDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(sqlEngine.OpenCalled).To(BeTrue())

				Expect(sqlEngine.OpenPassword).To(BeEquivalentTo(expectedMasterPassword))
			})
		})
	})

})
