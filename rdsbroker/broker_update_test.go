package rdsbroker_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/pivotal-cf/brokerapi/v8/domain"
	"github.com/pivotal-cf/brokerapi/v8/domain/apiresponses"

	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/rdsbroker/fakes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/rdsbroker"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	rdsfake "github.com/alphagov/paas-rds-broker/awsrds/fakes"
	sqlfake "github.com/alphagov/paas-rds-broker/sqlengine/fakes"
)

var _ = Describe("RDS Broker", func() {
	var (
		ctx context.Context

		rdsProperties1                RDSProperties
		rdsProperties2                RDSProperties
		rdsProperties3                RDSProperties
		rdsPropertiesPSQL10           RDSProperties
		rdsPropertiesPSQL11           RDSProperties
		rdsPropertiesPSQL12           RDSProperties
		rdsPropertiesPSQL12LowStorage RDSProperties
		plan1                         ServicePlan
		plan2                         ServicePlan
		plan3                         ServicePlan
		planPSQL10                    ServicePlan
		planPSQL11                    ServicePlan
		planPSQL12                    ServicePlan
		planPSQL12LowStorage          ServicePlan
		service1                      Service
		service2                      Service
		service3                      Service
		servicePSQL                   Service
		catalog                       Catalog

		config Config

		rdsInstance        *rdsfake.FakeRDSInstance
		existingDbInstance *rds.DBInstance

		sqlProvider *sqlfake.FakeProvider
		sqlEngine   *sqlfake.FakeSQLEngine

		testSink           *lagertest.TestSink
		logger             lager.Logger
		paramGroupSelector fakes.FakeParameterGroupSelector

		rdsBroker *RDSBroker

		allowUserProvisionParameters bool
		allowUserUpdateParameters    bool
		allowUserBindParameters      bool
		planUpdateable               bool
		skipFinalSnapshot            bool
		dbPrefix                     string
		brokerName                   string
		newParamGroupName            string
	)

	const (
		masterPasswordSeed   = "something-secret"
		instanceID           = "instance-id"
		bindingID            = "binding-id"
		dbInstanceIdentifier = "cf-instance-id"
		dbInstanceArn        = "arn:aws:rds:rds-region:1234567890:db:cf-instance-id"
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
		planUpdateable = true
		skipFinalSnapshot = true
		dbPrefix = "cf"
		brokerName = "mybroker"
		newParamGroupName = "originalParameterGroupName"

		rdsInstance = &rdsfake.FakeRDSInstance{}

		sqlProvider = &sqlfake.FakeProvider{}
		sqlEngine = &sqlfake.FakeSQLEngine{}
		sqlProvider.GetSQLEngineSQLEngine = sqlEngine

		rdsProperties1 = RDSProperties{
			DBInstanceClass:   stringPointer("db.m1.test"),
			Engine:            stringPointer("test-engine-one"),
			EngineVersion:     stringPointer("1.2.3"),
			AllocatedStorage:  int64Pointer(100),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			DefaultExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
		}

		rdsProperties2 = RDSProperties{
			DBInstanceClass:   stringPointer("db.m2.test"),
			Engine:            stringPointer("test-engine-one"),
			EngineVersion:     stringPointer("4.5.6"),
			AllocatedStorage:  int64Pointer(200),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			DefaultExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
		}

		rdsProperties3 = RDSProperties{
			DBInstanceClass:   stringPointer("db.m3.test"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("4.5.6"),
			AllocatedStorage:  int64Pointer(300),
			SkipFinalSnapshot: boolPointer(false),
			DefaultExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
		}

		rdsPropertiesPSQL10 = RDSProperties{
			DBInstanceClass:   stringPointer("db.t2.small"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("10"),
			AllocatedStorage:  int64Pointer(200),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			DefaultExtensions: []*string{
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
		}
		rdsPropertiesPSQL11 = RDSProperties{
			DBInstanceClass:   stringPointer("db.t3.small"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("11"),
			AllocatedStorage:  int64Pointer(200),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			DefaultExtensions: []*string{
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
		}
		rdsPropertiesPSQL12 = RDSProperties{
			DBInstanceClass:   stringPointer("db.t3.small"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("12"),
			AllocatedStorage:  int64Pointer(200),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			DefaultExtensions: []*string{
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
		}
		rdsPropertiesPSQL12LowStorage = RDSProperties{
			DBInstanceClass:   stringPointer("db.t3.small"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("12"),
			AllocatedStorage:  int64Pointer(100),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			DefaultExtensions: []*string{
				stringPointer("pg_stat_statements"),
			},
			AllowedExtensions: []*string{
				stringPointer("postgis"),
				stringPointer("pg_stat_statements"),
				stringPointer("postgres_super_extension"),
			},
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
		planPSQL10 = ServicePlan{
			ID:            "plan-psql10",
			Name:          "Plan PSQL 10",
			Description:   "",
			Free:          nil,
			Metadata:      nil,
			RDSProperties: rdsPropertiesPSQL10,
		}
		planPSQL11 = ServicePlan{
			ID:            "plan-psql11",
			Name:          "Plan PSQL 11",
			Description:   "",
			Free:          nil,
			Metadata:      nil,
			RDSProperties: rdsPropertiesPSQL11,
		}
		planPSQL12 = ServicePlan{
			ID:            "plan-psql12",
			Name:          "Plan PSQL 12",
			Description:   "",
			Free:          nil,
			Metadata:      nil,
			RDSProperties: rdsPropertiesPSQL12,
		}
		planPSQL12LowStorage = ServicePlan{
			ID:            "plan-psql12-low-storage",
			Name:          "Plan PSQL 12 (Low Storage)",
			Description:   "",
			Free:          nil,
			Metadata:      nil,
			RDSProperties: rdsPropertiesPSQL12LowStorage,
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
		servicePSQL = Service{
			ID:            "psql-service",
			Name:          "PSQL",
			Description:   "Provides Postgres",
			PlanUpdatable: planUpdateable,
			Plans: []ServicePlan{
				planPSQL10,
				planPSQL11,
				planPSQL12,
				planPSQL12LowStorage,
			},
		}

		catalog = Catalog{
			Services: []Service{service1, service2, service3, servicePSQL},
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

		paramGroupSelector = fakes.FakeParameterGroupSelector{}
		paramGroupSelector.SelectParameterGroupReturns(newParamGroupName, nil)

		rdsBroker = New(config, rdsInstance, sqlProvider, &paramGroupSelector, logger)

		existingDbInstance = &rds.DBInstance{
			DBParameterGroups: []*rds.DBParameterGroupStatus{
				&rds.DBParameterGroupStatus{
					DBParameterGroupName: aws.String("originalParameterGroupName"),
				},
			},
			Engine:        stringPointer("test-engine-one"),
			EngineVersion: stringPointer("1.2.3"),
		}
		rdsInstance.DescribeReturns(existingDbInstance, nil)
		rdsInstance.GetFullValidTargetVersionCalls(func(engine string, currentVersion string, targetVersionMoniker string) (string, error) {
			if currentVersion == "1.2.3" {
				return "4.5.6", nil
			} else {
				return "6.6.6", nil
			}
		})
	})

	Describe("Update", func() {
		var (
			updateDetails           domain.UpdateDetails
			acceptsIncomplete       bool
			properUpdateServiceSpec domain.UpdateServiceSpec
		)

		BeforeEach(func() {
			updateDetails = domain.UpdateDetails{
				ServiceID: "Service-2",
				PlanID:    "Plan-2",
				PreviousValues: domain.PreviousValues{
					PlanID:    "Plan-1",
					ServiceID: "Service-1",
					OrgID:     "organization-id",
					SpaceID:   "space-id",
				},
				RawParameters: json.RawMessage(`{}`),
			}
			acceptsIncomplete = true
			properUpdateServiceSpec = domain.UpdateServiceSpec{
				IsAsync: true,
			}

			rdsInstance.ModifyReturns(
				&rds.DBInstance{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
					DBInstanceArn:        aws.String(dbInstanceArn),
				},
				nil,
			)
		})

		It("returns the proper response", func() {
			updateServiceSpec, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(updateServiceSpec).To(Equal(properUpdateServiceSpec))
			Expect(err).ToNot(HaveOccurred())
		})

		It("makes the proper calls", func() {
			_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(err).ToNot(HaveOccurred())
			Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
			input := rdsInstance.ModifyArgsForCall(0)
			Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
			Expect(aws.StringValue(input.DBInstanceClass)).To(Equal("db.m2.test"))
		})

		It("sets the right tags", func() {
			_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(err).ToNot(HaveOccurred())

			Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
			arn, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
			Expect(arn).To(Equal(dbInstanceArn))

			tagsByName := awsrds.RDSTagsValues(tags)

			Expect(tagsByName).To(HaveKeyWithValue("Owner", "Cloud Foundry"))
			Expect(tagsByName).To(HaveKeyWithValue("Broker Name", "mybroker"))
			Expect(tagsByName).To(HaveKeyWithValue("Updated by", "AWS RDS Service Broker"))
			Expect(tagsByName).To(HaveKey("Updated at"))
			Expect(tagsByName).To(HaveKeyWithValue("Service ID", "Service-2"))
			Expect(tagsByName).To(HaveKeyWithValue("Plan ID", "Plan-2"))
			Expect(tagsByName).To(HaveKeyWithValue("chargeable_entity", instanceID))
		})

		Context("when custom update parameters are not provided", func() {
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
				rdsProperties2.AllocatedStorage = int64Pointer(100)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.Int64Value(input.AllocatedStorage)).To(Equal(int64(100)))
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				rdsProperties2.AutoMinorVersionUpgrade = boolPointer(true)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.AutoMinorVersionUpgrade)).To(BeTrue())
			})
		})

		Context("when has ApplyAtMaintenanceWindow", func() {

			It("applies immediately when apply_at_maintenance_window param is not given", func() {
				updateDetails.RawParameters = json.RawMessage(`{}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.ApplyImmediately)).To(BeTrue())
			})

			It("applies immediately when apply_at_maintenance_window param is false", func() {
				updateDetails.RawParameters = json.RawMessage(`{"apply_at_maintenance_window": false}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.ApplyImmediately)).To(BeTrue())
			})

			It("does not apply immediately when apply_at_maintenance_window param is true", func() {
				updateDetails.RawParameters = json.RawMessage(`{"apply_at_maintenance_window": true}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.ApplyImmediately)).To(BeFalse())
			})

		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				rdsProperties2.BackupRetentionPeriod = int64Pointer(7)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.Int64Value(input.BackupRetentionPeriod)).To(Equal(int64(7)))
			})

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("but has BackupRetentionPeriod Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`"backup_retention_period": 12}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
					input := rdsInstance.ModifyArgsForCall(0)
					Expect(aws.Int64Value(input.BackupRetentionPeriod)).To(Equal(int64(12)))
				})
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				rdsProperties2.CopyTagsToSnapshot = boolPointer(true)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.CopyTagsToSnapshot)).To(BeTrue())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				rdsProperties2.DBSecurityGroups = []*string{stringPointer("test-db-security-group")}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(input.DBSecurityGroups).To(Equal(
					[]*string{stringPointer("test-db-security-group")},
				))
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.DBSubnetGroupName = stringPointer("test-db-subnet-group-name")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.DBSubnetGroupName)).To(Equal("test-db-subnet-group-name"))
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				rdsProperties2.EngineVersion = stringPointer("1.2.3")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.EngineVersion)).To(Equal("1.2.3"))
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				rdsProperties2.Iops = int64Pointer(1000)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.Int64Value(input.Iops)).To(Equal(int64(1000)))
			})
		})

		Context("when has StorageEncrypted", func() {
			Context("when tries to enable StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = boolPointer(true)
					rdsProperties2.StorageEncrypted = boolPointer(true)
				})

				It("does nothing", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has KmsKeyID", func() {
			Context("when tries to set KmsKeyID to the same value", func() {
				BeforeEach(func() {
					rdsProperties1.KmsKeyID = stringPointer("some-kms-key-id")
					rdsProperties2.KmsKeyID = stringPointer("some-kms-key-id")
				})

				It("does nothing", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when storage encryption settings are updated", func() {
			Context("when tries to enable StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = boolPointer(false)
					rdsProperties2.StorageEncrypted = boolPointer(true)
				})

				It("fails noisily", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrEncryptionNotUpdateable))
				})
			})
			Context("when tries to disable StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = boolPointer(true)
					rdsProperties2.StorageEncrypted = boolPointer(false)
				})

				It("fails noisily", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrEncryptionNotUpdateable))
				})
			})
			Context("when changes KmsKeyID with StorageEncrypted enabled", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = boolPointer(true)
					rdsProperties2.StorageEncrypted = boolPointer(true)
					rdsProperties2.KmsKeyID = stringPointer("test-old-kms-key-id")
					rdsProperties2.KmsKeyID = stringPointer("test-new-kms-key-id")
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
				rdsProperties2.LicenseModel = stringPointer("test-license-model")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.LicenseModel)).To(Equal("test-license-model"))
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				rdsProperties2.MultiAZ = boolPointer(true)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.MultiAZ)).To(BeTrue())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				rdsProperties2.OptionGroupName = stringPointer("test-option-group-name")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.OptionGroupName)).To(Equal("test-option-group-name"))
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredBackupWindow = stringPointer("test-preferred-backup-window")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.PreferredBackupWindow)).To(Equal("test-preferred-backup-window"))
			})

			Context("but has PreferredBackupWindow Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`{"preferred_backup_window": "test-preferred-backup-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
					input := rdsInstance.ModifyArgsForCall(0)
					Expect(aws.StringValue(input.PreferredBackupWindow)).To(Equal("test-preferred-backup-window-parameter"))
				})
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				rdsProperties2.PreferredMaintenanceWindow = stringPointer("test-preferred-maintenance-window")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.PreferredMaintenanceWindow)).To(Equal("test-preferred-maintenance-window"))
			})

			Context("but has PreferredMaintenanceWindow Parameter", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
					input := rdsInstance.ModifyArgsForCall(0)
					Expect(aws.StringValue(input.PreferredMaintenanceWindow)).To(Equal("test-preferred-maintenance-window-parameter"))
				})
			})
		})

		Describe("handling SkipFinalSnapshot", func() {

			It("should not update the tag by default", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
				_, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				Expect(awsrds.RDSTagsValues(tags)).NotTo(HaveKey("SkipFinalSnapshot"))
			})

			It("should update the tag if the user requests it", func() {
				updateDetails.RawParameters = json.RawMessage(`{"skip_final_snapshot": true}`)

				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
				_, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				tagValues := awsrds.RDSTagsValues(tags)
				Expect(tagValues).To(HaveKey("SkipFinalSnapshot"))
				Expect(tagValues).To(HaveKeyWithValue("SkipFinalSnapshot", "true"))
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				rdsProperties2.PubliclyAccessible = boolPointer(true)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.BoolValue(input.PubliclyAccessible)).To(BeTrue())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				rdsProperties2.StorageType = stringPointer("test-storage-type")
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.StorageType)).To(Equal("test-storage-type"))
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				rdsProperties2.VpcSecurityGroupIds = []*string{stringPointer("test-vpc-security-group-ids")}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(input.VpcSecurityGroupIds).To(Equal(
					[]*string{stringPointer("test-vpc-security-group-ids")},
				))
			})
		})

		Context("when request does not accept incomplete", func() {
			BeforeEach(func() {
				acceptsIncomplete = false
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(apiresponses.ErrAsyncRequired))
			})
		})

		Context("when Parameters are not valid", func() {
			It("returns an error", func() {
				updateDetails.RawParameters = json.RawMessage(`not JSON`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
			})

			Context("and user update parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserUpdateParameters = false
				})

				It("does not return an error", func() {
					updateDetails.RawParameters = json.RawMessage(`not JSON`)
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			It("returns an error for extra params", func() {
				updateDetails.RawParameters = json.RawMessage(`{"foo": "bar"}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(MatchError(ContainSubstring(`unknown field "foo"`)))
				Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
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
				Expect(err).To(Equal(apiresponses.ErrPlanChangeNotSupported))
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
				rdsInstance.ModifyReturns(nil, errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					rdsInstance.ModifyReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(apiresponses.ErrInstanceDoesNotExist))
				})
			})

			Context("when RDS refuses the plan change", func() {
				BeforeEach(func() {
					rdsInstance.ModifyReturns(
						nil,
						awsrds.NewError(
							errors.New("InvalidParameterCombination: Cannot upgrade foo from X to Y"),
							awsrds.ErrCodeInvalidParameterCombination,
						),
					)
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError("InvalidParameterCombination: Cannot upgrade foo from X to Y"))

					errFR, ok := err.(*apiresponses.FailureResponse)
					Expect(ok).To(BeTrue())
					Expect(errFR.ValidatedStatusCode(logger)).To(
						Equal(http.StatusUnprocessableEntity),
					)
				})
			})
		})

		Context("when getting resource tags errors", func() {
			BeforeEach(func() {
				rdsInstance.GetResourceTagsReturns(nil, errors.New("operation failed"))
			})

			It("returns an error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when enabling extensions", func() {
			BeforeEach(func() {
				updateDetails.RawParameters = json.RawMessage(`{"enable_extensions": ["postgres_super_extension"]}`)
			})

			It("accepts the enable_extensions parameter when there is no plan change", func() {
				updateDetails.PlanID = "Plan-1"
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
			})

			It("fails if the request includes a plan change", func() {
				updateDetails.PlanID = "Plan-2"
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError("Invalid to enable extensions and update plan in the same command"))
				Expect(rdsInstance.RebootCallCount()).To(Equal(0))
				Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
			})
		})

		Context("when disabling extensions", func() {
			BeforeEach(func() {
				updateDetails.RawParameters = json.RawMessage(`{"disable_extensions": ["postgres_super_extension"]}`)
			})

			It("accepts the disable_extensions parameter when there is no plan change", func() {
				updateDetails.PlanID = "Plan-1"
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
			})

			It("fails if the request includes a plan change", func() {
				updateDetails.PlanID = "Plan-2"
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError("Invalid to disable extensions and update plan in the same command"))
				Expect(rdsInstance.RebootCallCount()).To(Equal(0))
				Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
			})
		})

		Context("when the plan is changing", func() {
			BeforeEach(func() {
				rdsProperties1.EngineVersion = stringPointer("1.2.3")
				rdsProperties2.EngineVersion = stringPointer("4.5.6")
				newParamGroupName = "mockedOutReturnValueOfSelectParameterGroupIndicatingUseOfEngineVersion4.5.6"

				updateDetails = domain.UpdateDetails{
					ServiceID: "Service-1",
					PlanID:    "Plan-2",
					PreviousValues: domain.PreviousValues{
						PlanID:    "Plan-1",
						ServiceID: "Service-1",
						OrgID:     "organization-id",
						SpaceID:   "space-id",
					},
				}
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
				servicePlan, _ := paramGroupSelector.SelectParameterGroupArgsForCall(0)
				Expect(servicePlan).To(Equal(plan2))

				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.EngineVersion)).To(Equal("4.5.6"))
				Expect(aws.StringValue(input.DBParameterGroupName)).To(Equal(newParamGroupName))
			})

			It("cannot have the version downgraded", func() {
				updateDetails.PlanID = planPSQL10.ID
				updateDetails.ServiceID = servicePSQL.ID
				updateDetails.PreviousValues = domain.PreviousValues{
					PlanID:    planPSQL12.ID,
					ServiceID: servicePSQL.ID,
					OrgID:     updateDetails.PreviousValues.OrgID,
					SpaceID:   updateDetails.PreviousValues.SpaceID,
				}

				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrCannotDowngradeVersion.Error()))
			})

			It("cannot have the storage downgraded", func() {
				updateDetails.PlanID = planPSQL12LowStorage.ID
				updateDetails.ServiceID = servicePSQL.ID
				updateDetails.PreviousValues = domain.PreviousValues{
					PlanID:    planPSQL12.ID,
					ServiceID: servicePSQL.ID,
					OrgID:     updateDetails.PreviousValues.OrgID,
					SpaceID:   updateDetails.PreviousValues.SpaceID,
				}

				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrCannotDowngradeStorage.Error()))
			})

			It("cannot be changed by more than 1 major version", func() {
				updateDetails.PlanID = planPSQL12.ID
				updateDetails.ServiceID = servicePSQL.ID
				updateDetails.PreviousValues = domain.PreviousValues{
					PlanID:    planPSQL10.ID,
					ServiceID: servicePSQL.ID,
					OrgID:     updateDetails.PreviousValues.OrgID,
					SpaceID:   updateDetails.PreviousValues.SpaceID,
				}

				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrCannotSkipMajorVersion.Error()))
			})
		})

		Context("if an extension is in both enable_extensions and disable_extension", func() {
			It("returns an error", func() {
				updateDetails.RawParameters = json.RawMessage(`{"disable_extensions": ["postgres_super_extension"], "enable_extensions": ["postgres_super_extension"]}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(MatchError("postgres_super_extension is set in both enable_extensions and disable_extensions"))
			})
		})

		Context("when reboot is set to true", func() {
			BeforeEach(func() {
				updateDetails = domain.UpdateDetails{
					ServiceID: "Service-1",
					PlanID:    "Plan-1",
					PreviousValues: domain.PreviousValues{
						PlanID:    "Plan-1",
						ServiceID: "Service-1",
						OrgID:     "organization-id",
						SpaceID:   "space-id",
					},
					RawParameters: json.RawMessage(`{ "reboot": true }`),
				}
			})

			It("returns the proper response", func() {
				updateServiceSpec, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(updateServiceSpec).To(Equal(properUpdateServiceSpec))
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.RebootCallCount()).To(Equal(1))
				input := rdsInstance.RebootArgsForCall(0)
				Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
			})

			It("passes the force failover option", func() {
				updateDetails.RawParameters = json.RawMessage(`{ "reboot": true, "force_failover": true }`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.RebootCallCount()).To(Equal(1))
				input := rdsInstance.RebootArgsForCall(0)
				Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				Expect(aws.BoolValue(input.ForceFailover)).To(BeTrue())
				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
			})

			It("fails if the reboot include a plan change", func() {
				updateDetails.RawParameters = json.RawMessage(`{ "reboot": true, "force_failover": true }`)
				updateDetails.PlanID = "Plan-2"
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError("Invalid to reboot and update plan in the same command"))
				Expect(rdsInstance.RebootCallCount()).To(Equal(0))
				Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
			})
		})

		Context("when extension is added", func() {
			BeforeEach(func() {
				updateDetails = domain.UpdateDetails{
					ServiceID: "Service-1",
					PlanID:    "Plan-1",
					PreviousValues: domain.PreviousValues{
						PlanID:    "Plan-1",
						ServiceID: "Service-1",
						OrgID:     "organization-id",
						SpaceID:   "space-id",
					},
					RawParameters: json.RawMessage(`{ "reboot": true }`),
				}

				dbTags := map[string]string{
					awsrds.TagExtensions: "postgis:pg_stat_statements",
				}
				rdsInstance.GetResourceTagsReturns(awsrds.BuildRDSTags(dbTags), nil)
			})

			It("successfully sets an extension", func() {
				updateDetails.RawParameters = json.RawMessage(`{"enable_extensions": ["postgres_super_extension"]}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.DBParameterGroupName)).To(Equal(newParamGroupName))

				Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
				_, extensions := paramGroupSelector.SelectParameterGroupArgsForCall(0)
				Expect(extensions).To(ContainElement("postgres_super_extension"))
				Expect(extensions).To(ContainElement("postgis"))
				Expect(extensions).To(ContainElement("pg_stat_statements"))
				Expect(extensions).To(HaveLen(3))
				Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
				_, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				Expect(tags).To(ContainElement(&rds.Tag{
					Key:   aws.String("Extensions"),
					Value: aws.String("postgis:pg_stat_statements:postgres_super_extension"),
				}))
			})

			It("ignores an extension that has already been enabled", func() {
				updateDetails.RawParameters = json.RawMessage(`{"enable_extensions": ["pg_stat_statements"]}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				_, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				Expect(tags).To(ContainElement(&rds.Tag{
					Key:   aws.String("Extensions"),
					Value: aws.String("postgis:pg_stat_statements"),
				}))

				_, extensions := paramGroupSelector.SelectParameterGroupArgsForCall(0)
				Expect(extensions).To(HaveLen(2))
			})

			It("checks if the extension is supported", func() {
				updateDetails.RawParameters = json.RawMessage(`{"enable_extensions": ["noext"]}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).To(MatchError("noext is not supported"))
			})

			Context("when the parameter group is updated", func() {
				BeforeEach(func() {
					newParamGroupName = "updatedParamGroupName"
				})

				It("enables an extension successfully if reboot is set to true", func() {
					updateDetails.RawParameters = json.RawMessage(`{"enable_extensions": ["postgres_super_extension"], "reboot": true}`)
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RebootCallCount()).To(Equal(0))
				})

				It("fails when reboot isn't set and an extension that requires one is set", func() {
					updateDetails.RawParameters = json.RawMessage(`{"enable_extensions": ["postgres_super_extension"]}`)
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(MatchError("The requested extensions require the instance to be manually rebooted. Please re-run update service with reboot set to true"))
				})
			})
		})

		Context("when an extension is removed", func() {
			BeforeEach(func() {
				updateDetails = domain.UpdateDetails{
					ServiceID: "Service-1",
					PlanID:    "Plan-1",
					PreviousValues: domain.PreviousValues{
						PlanID:    "Plan-1",
						ServiceID: "Service-1",
						OrgID:     "organization-id",
						SpaceID:   "space-id",
					},
					RawParameters: json.RawMessage(`{ "reboot": true }`),
				}

				dbTags := map[string]string{
					awsrds.TagExtensions: "postgis:pg_stat_statements:postgres_super_extension",
				}
				rdsInstance.GetResourceTagsReturns(awsrds.BuildRDSTags(dbTags), nil)
				newParamGroupName = "updatedParamGroupName"
			})

			It("successfully removes an extension", func() {
				updateDetails.RawParameters = json.RawMessage(`{"disable_extensions": ["postgres_super_extension"], "reboot": true}`)
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.DBParameterGroupName)).To(Equal(newParamGroupName))

				Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
				_, extensions := paramGroupSelector.SelectParameterGroupArgsForCall(0)
				Expect(extensions).ToNot(ContainElement("postgres_super_extension"))
				Expect(extensions).To(ContainElement("postgis"))
				Expect(extensions).To(ContainElement("pg_stat_statements"))
				Expect(extensions).To(HaveLen(2))
				Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
				_, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				Expect(tags).To(ContainElement(&rds.Tag{
					Key:   aws.String("Extensions"),
					Value: aws.String("postgis:pg_stat_statements"),
				}))
				Expect(rdsInstance.RebootCallCount()).To(Equal(0))
			})
		})

		Context("when upgrade minor version to latest", func() {
			BeforeEach(func() {
				updateDetails.RawParameters = json.RawMessage(`{"update_minor_version_to_latest": true}`)
				updateDetails.PlanID = updateDetails.PreviousValues.PlanID
			})

			JustBeforeEach(func() {
				existingDbInstance.Engine = aws.String("postgres")
				existingDbInstance.EngineVersion = aws.String("11")
			})

			It("successfully upgrades the plan", func() {
				rdsInstance.GetLatestMinorVersionReturns(stringPointer("11.999"), nil)

				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).NotTo(HaveOccurred())

				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.EngineVersion)).To(Equal("11.999"))
			})

			Context("when reboot is specified", func() {
				BeforeEach(func() {
					updateDetails.RawParameters = json.RawMessage(`{"update_minor_version_to_latest": true, "reboot": true}`)
				})

				It("fails", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(
						"Cannot reboot and upgrade minor version to latest at the same time",
					))
				})
			})

			Context("when a plan ID is specified", func() {
				BeforeEach(func() {
					updateDetails.PlanID = "Plan-2"
					updateDetails.RawParameters = json.RawMessage(`{"update_minor_version_to_latest": true}`)
				})

				It("fails", func() {
					_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(
						"Cannot specify a version and upgrade minor version to latest at the same time",
					))
				})
			})
		})
	})
})
