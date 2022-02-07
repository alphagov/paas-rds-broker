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

	"github.com/alphagov/paas-rds-broker/rdsbroker/fakes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/alphagov/paas-rds-broker/awsrds"
	. "github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/sqlengine"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/pivotal-cf/brokerapi"

	rdsfake "github.com/alphagov/paas-rds-broker/awsrds/fakes"
	sqlfake "github.com/alphagov/paas-rds-broker/sqlengine/fakes"
)

var _ = Describe("RDS Broker", func() {
	var (
		ctx context.Context

		rdsProperties1 RDSProperties
		rdsProperties2 RDSProperties
		rdsProperties3 RDSProperties
		rdsProperties4 RDSProperties
		rdsProperties5 RDSProperties
		plan1          ServicePlan
		plan2          ServicePlan
		plan3          ServicePlan
		plan4          ServicePlan
		plan5          ServicePlan
		service1       Service
		service2       Service
		service3       Service
		catalog        Catalog

		config Config

		rdsInstance *rdsfake.FakeRDSInstance

		sqlProvider *sqlfake.FakeProvider
		sqlEngine   *sqlfake.FakeSQLEngine

		testSink           *lagertest.TestSink
		logger             lager.Logger
		paramGroupSelector fakes.FakeParameterGroupSelector

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
		serviceBindable = true
		planUpdateable = true
		skipFinalSnapshot = true
		dbPrefix = "cf"
		brokerName = "mybroker"

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
			MultiAZ:           boolPointer(false),
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
			Engine:            stringPointer("test-engine-two"),
			EngineVersion:     stringPointer("4.5.6"),
			AllocatedStorage:  int64Pointer(200),
			SkipFinalSnapshot: boolPointer(skipFinalSnapshot),
			MultiAZ:           boolPointer(false),
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
			MultiAZ:           boolPointer(false),
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

		rdsProperties4 = RDSProperties{
			DBInstanceClass:   stringPointer("db.m3.test"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("5.6.7"),
			AllocatedStorage:  int64Pointer(300),
			SkipFinalSnapshot: boolPointer(false),
			MultiAZ:           boolPointer(false),
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

		rdsProperties5 = RDSProperties{
			DBInstanceClass:   stringPointer("db.m3.test"),
			Engine:            stringPointer("postgres"),
			EngineVersion:     stringPointer("5.6.7"),
			AllocatedStorage:  int64Pointer(400),
			SkipFinalSnapshot: boolPointer(false),
			MultiAZ:           boolPointer(false),
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
		plan4 = ServicePlan{
			ID:            "Plan-4",
			Name:          "Plan 4",
			Description:   "This is the Plan 4",
			RDSProperties: rdsProperties4,
		}
		plan5 = ServicePlan{
			ID:            "Plan-5",
			Name:          "Plan 5",
			Description:   "This is the Plan 5",
			RDSProperties: rdsProperties5,
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
			Plans:         []ServicePlan{plan3, plan4, plan5},
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

		paramGroupSelector = fakes.FakeParameterGroupSelector{}
		paramGroupSelector.SelectParameterGroupReturns(dbPrefix+"-postgres10-"+brokerName, nil)

		rdsBroker = New(config, rdsInstance, sqlProvider, &paramGroupSelector, logger)

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
						brokerapi.ServicePlan{
							ID:          "Plan-4",
							Name:        "Plan 4",
							Description: "This is the Plan 4",
						},
					},
				},
			}
		})

		It("returns the proper CatalogResponse", func() {
			brokerCatalog, err := rdsBroker.Services(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(brokerCatalog).To(Equal(properCatalogResponse))
		})

		It("brokerapi integration returns the proper CatalogResponse", func() {
			var err error

			recorder := httptest.NewRecorder()

			req, _ := http.NewRequest("GET", "http://example.com/v2/catalog", nil)
			req.Header.Set("X-Broker-API-Version", "2.14")
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

		Context("when restoring from a point in time", func() {
			var (
				restoreFromPointInTimeInstanceGUID  string
				restoreFromPointInTimeDBInstanceID  string
				restoreFromPointInTimeDBInstanceARN string
				dbIdentifierTags                    map[string]string
			)

			BeforeEach(func() {
				rdsProperties1.Engine = stringPointer("postgres")
				restoreFromPointInTimeInstanceGUID = "guid-of-origin-instance"
				restoreFromPointInTimeDBInstanceID = dbPrefix + "-guid-of-origin-instance"
				restoreFromPointInTimeDBInstanceARN = "arn:aws:rds:rds-region:1234567890:db:" + restoreFromPointInTimeDBInstanceID
				provisionDetails.RawParameters = json.RawMessage(`{"restore_from_point_in_time_of": "` + restoreFromPointInTimeInstanceGUID + `"}`)

				dbIdentifierTags = map[string]string{
					"Space ID":        "space-id",
					"Organization ID": "organization-id",
					"Plan ID":         "Plan-1",
				}
			})

			JustBeforeEach(func() {
				rdsInstance.DescribeReturns(&rds.DBInstance{
					DBInstanceArn:        aws.String(restoreFromPointInTimeDBInstanceARN),
					DBInstanceIdentifier: aws.String(restoreFromPointInTimeDBInstanceID),
				}, nil)
				rdsInstance.GetResourceTagsReturns(awsrds.BuildRDSTags(dbIdentifierTags), nil)
			})

			Context("and the restore_from_latest_snapshot_of also present", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"restore_from_latest_snapshot_of": "abc", "restore_from_point_in_time_of": "def"}`)
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("Cannot use both restore_from_latest_snapshot_of and restore_from_point_in_time_of at the same time"))
				})
			})

			Context("when the engine is not 'postgres'", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = stringPointer("some-other-engine")
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("not supported for engine"))
				})
			})

			Context("and the restore_from_point_in_time_of is an empty string", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"restore_from_point_in_time_of": ""}`)
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("Invalid guid"))
				})
			})

			Context("and the instance does not exist", func() {
				JustBeforeEach(func() {
					rdsInstance.GetResourceTagsReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("Cannot find instance " + restoreFromPointInTimeDBInstanceARN))
				})
			})

			Context("when the snapshot is in a different org", func() {
				BeforeEach(func() {
					dbIdentifierTags["Organization ID"] = "different-organization-id"
				})

				It("should fail to restore", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("if it is using a different plan", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"restore_from_point_in_time_of": "a-guid"}`)
					dbIdentifierTags["Plan ID"] = "different-plan-id"
				})

				It("should fail to restore", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("if it is using an invalid date", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"restore_from_point_in_time_of": "a-guid", "restore_from_point_in_time_before": "2006-01-01"}`)
				})

				It("returns the correct error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).Should(ContainSubstring("Parameter restore_from_point_in_time_before should be a date and a time"))
				})
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.RestoreToPointInTimeCallCount()).To(Equal(1))
				input := rdsInstance.RestoreToPointInTimeArgsForCall(0)
				Expect(aws.StringValue(input.TargetDBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				Expect(aws.StringValue(input.SourceDBInstanceIdentifier)).To(Equal(restoreFromPointInTimeDBInstanceID))
				Expect(aws.StringValue(input.DBInstanceClass)).To(Equal("db.m1.test"))
				Expect(aws.StringValue(input.Engine)).To(Equal("postgres"))
				Expect(aws.StringValue(input.DBName)).To(BeEmpty())
				Expect(aws.BoolValue(input.UseLatestRestorableTime)).To(Equal(true))
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.GetResourceTagsCallCount()).To(Equal(1))
				dbARN, _ := rdsInstance.GetResourceTagsArgsForCall(0)
				Expect(dbARN).To(Equal(restoreFromPointInTimeDBInstanceARN))
			})

			It("sets the right tags", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.RestoreToPointInTimeCallCount()).To(Equal(1))
				input := rdsInstance.RestoreToPointInTimeArgsForCall(0)

				tagsByName := awsrds.RDSTagsValues(input.Tags)
				Expect(tagsByName).To(HaveKeyWithValue("Owner", "Cloud Foundry"))
				Expect(tagsByName).To(HaveKeyWithValue("Restored by", "AWS RDS Service Broker"))
				Expect(tagsByName).To(HaveKey("Restored at"))
				Expect(tagsByName).To(HaveKeyWithValue("Service ID", "Service-1"))
				Expect(tagsByName).To(HaveKeyWithValue("Plan ID", "Plan-1"))
				Expect(tagsByName).To(HaveKeyWithValue("Organization ID", "organization-id"))
				Expect(tagsByName).To(HaveKeyWithValue("Space ID", "space-id"))
				Expect(tagsByName).To(HaveKeyWithValue("Restored From Database", restoreFromPointInTimeDBInstanceID))
				Expect(tagsByName).To(HaveKeyWithValue("PendingResetUserPassword", "true"))
				Expect(tagsByName).To(HaveKeyWithValue("PendingUpdateSettings", "true"))
				Expect(tagsByName).To(HaveKeyWithValue("chargeable_entity", instanceID))
			})

			Context("when restoring before a particular point in time", func() {
				var (
					restoreTime time.Time
				)

				BeforeEach(func() {
					restoreTime = time.Now().UTC().Add(-1 * time.Hour)
					provisionDetails.RawParameters = json.RawMessage(
						`{` +
							`"restore_from_point_in_time_of": "` + restoreFromPointInTimeInstanceGUID + `",` +
							`"restore_from_point_in_time_before": "` + restoreTime.Format("2006-01-02 15:04:05") + `"` +
							`}`,
					)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())

					Expect(rdsInstance.RestoreToPointInTimeCallCount()).To(Equal(1))
					input := rdsInstance.RestoreToPointInTimeArgsForCall(0)
					Expect(aws.StringValue(input.TargetDBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
					Expect(aws.StringValue(input.SourceDBInstanceIdentifier)).To(Equal(restoreFromPointInTimeDBInstanceID))
					Expect(aws.TimeValue(input.RestoreTime)).To(BeTemporally("~", restoreTime, 1*time.Second))
					Expect(input.UseLatestRestorableTime).To(BeNil())
					Expect(err).ToNot(HaveOccurred())
				})

				It("sets the right tags", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())

					Expect(rdsInstance.RestoreToPointInTimeCallCount()).To(Equal(1))
					input := rdsInstance.RestoreToPointInTimeArgsForCall(0)

					tagsByName := awsrds.RDSTagsValues(input.Tags)
					Expect(tagsByName).To(HaveKeyWithValue("Restored From Database", restoreFromPointInTimeDBInstanceID))
					tagTime := tagsByName["Restored From Time"]
					tagParsedTime, err := time.Parse(time.RFC3339, tagTime)
					Expect(err).NotTo(HaveOccurred())
					Expect(tagParsedTime).To(BeTemporally("~", restoreTime, 1*time.Second))
				})
			})
		})

		Context("when restoring from a snapshot", func() {
			var (
				restoreFromSnapshotInstanceGUID  string
				restoreFromSnapshotDBInstanceID  string
				restoreFromSnapshotDBSnapshotArn string
				dbSnapshotTags                   map[string]string
			)

			JustBeforeEach(func() {
				rdsInstance.DescribeSnapshotsReturns([]*rds.DBSnapshot{
					{
						DBSnapshotIdentifier: aws.String(restoreFromSnapshotDBInstanceID + "-1"),
						DBSnapshotArn:        aws.String(restoreFromSnapshotDBSnapshotArn + "-1"),
						DBInstanceIdentifier: aws.String(restoreFromSnapshotDBInstanceID),
						SnapshotCreateTime:   aws.Time(time.Now()),
					},
					{
						DBSnapshotIdentifier: aws.String(restoreFromSnapshotDBInstanceID + "-2"),
						DBSnapshotArn:        aws.String(restoreFromSnapshotDBSnapshotArn + "-2"),
						DBInstanceIdentifier: aws.String(restoreFromSnapshotDBInstanceID),
						SnapshotCreateTime:   aws.Time(time.Now().Add(-1 * 24 * time.Hour)),
					},
					{
						DBSnapshotIdentifier: aws.String(restoreFromSnapshotDBInstanceID + "-3"),
						DBSnapshotArn:        aws.String(restoreFromSnapshotDBSnapshotArn + "-3"),
						DBInstanceIdentifier: aws.String(restoreFromSnapshotDBInstanceID),
						SnapshotCreateTime:   aws.Time(time.Now().Add(-1 * 3 * 24 * time.Hour)),
					},
				}, nil)

				rdsInstance.GetResourceTagsReturns(awsrds.BuildRDSTags(dbSnapshotTags), nil)
			})

			Context("without a restore_from_latest_snapshot_before modifier", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = stringPointer("postgres")
					restoreFromSnapshotInstanceGUID = "guid-of-origin-instance"
					restoreFromSnapshotDBInstanceID = dbPrefix + "-" + restoreFromSnapshotInstanceGUID
					restoreFromSnapshotDBSnapshotArn = "arn:aws:rds:rds-region:1234567890:snapshot:cf-instance-id"
					provisionDetails.RawParameters = json.RawMessage(`{"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `"}`)
					dbSnapshotTags = map[string]string{
						"Space ID":        "space-id",
						"Organization ID": "organization-id",
						"Plan ID":         "Plan-1",
					}
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(rdsInstance.DescribeSnapshotsCallCount()).To(Equal(1))
					id := rdsInstance.DescribeSnapshotsArgsForCall(0)
					Expect(id).To(Equal(restoreFromSnapshotDBInstanceID))

					Expect(rdsInstance.RestoreCallCount()).To(Equal(1))
					input := rdsInstance.RestoreArgsForCall(0)
					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
					Expect(aws.StringValue(input.DBSnapshotIdentifier)).To(Equal(restoreFromSnapshotDBInstanceID + "-1"))
					Expect(aws.StringValue(input.DBInstanceClass)).To(Equal("db.m1.test"))
					Expect(aws.StringValue(input.Engine)).To(Equal("postgres"))
					Expect(aws.StringValue(input.DBName)).To(BeEmpty())
					Expect(err).ToNot(HaveOccurred())
				})

				It("sets the right tags", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)

					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.DescribeSnapshotsCallCount()).To(Equal(1))
					id := rdsInstance.DescribeSnapshotsArgsForCall(0)
					Expect(id).To(Equal(restoreFromSnapshotDBInstanceID))

					Expect(rdsInstance.RestoreCallCount()).To(Equal(1))
					input := rdsInstance.RestoreArgsForCall(0)

					tagsByName := awsrds.RDSTagsValues(input.Tags)
					Expect(tagsByName).To(HaveKeyWithValue("Owner", "Cloud Foundry"))
					Expect(tagsByName).To(HaveKeyWithValue("Restored by", "AWS RDS Service Broker"))
					Expect(tagsByName).To(HaveKey("Restored at"))
					Expect(tagsByName).To(HaveKeyWithValue("Service ID", "Service-1"))
					Expect(tagsByName).To(HaveKeyWithValue("Plan ID", "Plan-1"))
					Expect(tagsByName).To(HaveKeyWithValue("Organization ID", "organization-id"))
					Expect(tagsByName).To(HaveKeyWithValue("Space ID", "space-id"))
					Expect(tagsByName).To(HaveKeyWithValue("Restored From Snapshot", restoreFromSnapshotDBInstanceID+"-1"))
					Expect(tagsByName).To(HaveKeyWithValue("PendingResetUserPassword", "true"))
					Expect(tagsByName).To(HaveKeyWithValue("PendingUpdateSettings", "true"))
					Expect(tagsByName).To(HaveKeyWithValue("chargeable_entity", instanceID))
				})

				It("selects the latest snapshot", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(rdsInstance.RestoreCallCount()).To(Equal(1))
					input := rdsInstance.RestoreArgsForCall(0)
					Expect(aws.StringValue(input.DBSnapshotIdentifier)).To(Equal(restoreFromSnapshotDBInstanceID + "-1"))
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
						rdsInstance.RestoreReturns(errors.New("operation failed"))
					})

					It("returns the proper error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("operation failed"))
					})
				})

				Context("and no snapshots are found", func() {
					JustBeforeEach(func() {
						rdsInstance.DescribeSnapshotsReturns([]*rds.DBSnapshot{}, nil)
					})

					It("returns the correct error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).Should(ContainSubstring("No snapshots found"))
					})
				})

				Context("when the engine is not 'postgres'", func() {
					BeforeEach(func() {
						rdsProperties1.Engine = stringPointer("some-other-engine")
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

				Context("when the snapshot had extensions enabled", func() {
					It("sets the same extensions on the new database", func() {
						dbSnapshotTags[awsrds.TagExtensions] = "foo:bar"
						rdsInstance.GetResourceTagsReturns(awsrds.BuildRDSTags(dbSnapshotTags), nil)

						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)

						Expect(err).ToNot(HaveOccurred())

						Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
						_, extensions := paramGroupSelector.SelectParameterGroupArgsForCall(0)
						Expect(extensions).To(ContainElement("foo"))
						Expect(extensions).To(ContainElement("bar"))
					})

					Context("when the user passes extensions to set", func() {
						BeforeEach(func() {
							provisionDetails.RawParameters = json.RawMessage(`{"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `", "enable_extensions": ["postgres_super_extension"]}`)
						})
						It("adds those extensions to the set of extensions on the snapshot", func() {
							dbSnapshotTags[awsrds.TagExtensions] = "foo:bar"
							rdsInstance.GetResourceTagsReturns(awsrds.BuildRDSTags(dbSnapshotTags), nil)

							_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)

							Expect(err).ToNot(HaveOccurred())

							Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
							_, extensions := paramGroupSelector.SelectParameterGroupArgsForCall(0)
							Expect(extensions).To(ContainElement("foo"))
							Expect(extensions).To(ContainElement("bar"))
							Expect(extensions).To(ContainElement("postgres_super_extension"))
						})
					})
				})
			})

			Context("without a restore_from_latest_snapshot_before modifier", func() {
				BeforeEach(func() {
					rdsProperties1.Engine = stringPointer("postgres")
					restoreFromSnapshotInstanceGUID = "guid-of-origin-instance"
					restoreFromSnapshotDBInstanceID = dbPrefix + "-" + restoreFromSnapshotInstanceGUID
					restoreFromSnapshotDBSnapshotArn = "arn:aws:rds:rds-region:1234567890:snapshot:cf-instance-id"
					provisionDetails.RawParameters = json.RawMessage(
						`{` +
							`"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `",` +
							`"restore_from_latest_snapshot_before": "` + time.Now().Add(-1*time.Hour).Format("2006-01-02 15:04:05") + `"` +
							`}`,
					)
					dbSnapshotTags = map[string]string{
						"Space ID":        "space-id",
						"Organization ID": "organization-id",
						"Plan ID":         "Plan-1",
					}
				})

				It("does not select the latest snapshot", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(rdsInstance.RestoreCallCount()).To(Equal(1))
					input := rdsInstance.RestoreArgsForCall(0)
					Expect(aws.StringValue(input.DBSnapshotIdentifier)).To(Equal(restoreFromSnapshotDBInstanceID + "-2"))
					Expect(err).ToNot(HaveOccurred())
				})

				Context("and the restore_from_latest_snapshot_before excludes all snapshots", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(
							`{` +
								`"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `",` +
								`"restore_from_latest_snapshot_before": "` + time.Now().Add(-1*7*24*time.Hour).Format("2006-01-02 15:04:05") + `"` +
								`}`,
						)
					})

					It("returns the correct error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).Should(ContainSubstring("No snapshots found"))
					})
				})

				Context("and the restore_from_latest_snapshot_before is an empty string", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(
							`{` +
								`"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `",` +
								`"restore_from_latest_snapshot_before": ""` +
								`}`,
						)
					})

					It("returns the correct error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).Should(ContainSubstring("Parameter restore_from_latest_snapshot_before must not be empty"))
					})
				})

				Context("and the restore_from_latest_snapshot_before is a date without a time", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(
							`{` +
								`"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `",` +
								`"restore_from_latest_snapshot_before": "2006-02-01"` +
								`}`,
						)
					})

					It("returns the correct error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).Should(ContainSubstring("Parameter restore_from_latest_snapshot_before should be a date and a time"))
					})
				})

				Context("and the restore_from_latest_snapshot_before is a time without a date", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(
							`{` +
								`"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `",` +
								`"restore_from_latest_snapshot_before": "20:43:15"` +
								`}`,
						)
					})

					It("returns the correct error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).Should(ContainSubstring("Parameter restore_from_latest_snapshot_before should be a date and a time"))
					})
				})

				Context("and the restore_from_latest_snapshot_of is not set", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(
							`{` +
								`"restore_from_latest_snapshot_before": "20:43:15"` +
								`}`,
						)
					})

					It("returns the correct error", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).Should(ContainSubstring("Parameter restore_from_latest_snapshot_before should be used with restore_from_latest_snapshot_of"))
					})
				})
			})
		})

		Context("when creating a new service instance", func() {
			It("makes the proper calls", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.CreateCallCount()).To(Equal(1))
				input := rdsInstance.CreateArgsForCall(0)
				Expect(input).ToNot(BeNil())

				Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				Expect(aws.StringValue(input.DBInstanceClass)).To(Equal("db.m1.test"))
				Expect(aws.StringValue(input.Engine)).To(Equal("test-engine-one"))
				Expect(aws.StringValue(input.DBName)).To(Equal(dbName))
				Expect(aws.StringValue(input.MasterUsername)).ToNot(BeEmpty())
				Expect(aws.StringValue(input.MasterUserPassword)).To(Equal(masterUserPassword))

			})

			It("sets the right tags", func() {
				jsonData := []byte(`{"enable_extensions": ["postgis", "pg_stat_statements"]}`)
				rawparams := (*json.RawMessage)(&jsonData)
				provisionDetails.RawParameters = *rawparams

				provisionDetails.ServiceID = "Service-3"
				provisionDetails.PlanID = "Plan-3"

				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.CreateCallCount()).To(Equal(1))
				input := rdsInstance.CreateArgsForCall(0)
				Expect(input).ToNot(BeNil())

				tagsByName := awsrds.RDSTagsValues(input.Tags)

				Expect(tagsByName).To(HaveKeyWithValue("Owner", "Cloud Foundry"))
				Expect(tagsByName).To(HaveKeyWithValue("Created by", "AWS RDS Service Broker"))
				Expect(tagsByName).To(HaveKey("Created at"))
				Expect(tagsByName).To(HaveKeyWithValue("Service ID", "Service-3"))
				Expect(tagsByName).To(HaveKeyWithValue("Plan ID", "Plan-3"))
				Expect(tagsByName).To(HaveKeyWithValue("Organization ID", "organization-id"))
				Expect(tagsByName).To(HaveKeyWithValue("Space ID", "space-id"))
				Expect(tagsByName).To(HaveKeyWithValue("Extensions", "postgis:pg_stat_statements"))
				Expect(tagsByName).To(HaveKeyWithValue("chargeable_entity", instanceID))
			})

			It("does not set a 'Restored From Snapshot' tag", func() {
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.CreateCallCount()).To(Equal(1))
				input := rdsInstance.CreateArgsForCall(0)
				Expect(input).ToNot(BeNil())

				tagsByName := awsrds.RDSTagsValues(input.Tags)
				Expect(tagsByName).ToNot(HaveKey("Restored From Snapshot"))
			})

			It("sets the parameter group from the parameter groups selector", func() {
				paramGroupSelector.SelectParameterGroupReturns("expected", nil)
				_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())

				Expect(rdsInstance.CreateCallCount()).To(Equal(1))
				input := rdsInstance.CreateArgsForCall(0)

				Expect(aws.StringValue(input.DBParameterGroupName)).To(Equal("expected"))
			})

			Context("creates a SkipFinalSnapshot tag", func() {
				Context("with a plan that doesn't specify SkipFinalSnapshot", func() {
					BeforeEach(func() {
						rdsProperties1.SkipFinalSnapshot = nil
					})

					It("sets the tag to false by default", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())

						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(input).ToNot(BeNil())

						tagsByName := awsrds.RDSTagsValues(input.Tags)
						Expect(tagsByName).To(HaveKeyWithValue("SkipFinalSnapshot", "false"))
					})

					It("allows the user to override this", func() {
						provisionDetails.RawParameters = json.RawMessage(`{"skip_final_snapshot": true}`)

						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())

						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(input).ToNot(BeNil())

						tagsByName := awsrds.RDSTagsValues(input.Tags)
						Expect(tagsByName).To(HaveKeyWithValue("SkipFinalSnapshot", "true"))
					})
				})

				Context("with a plan that specifies SkipFinalSnapshot", func() {
					BeforeEach(func() {
						rdsProperties1.SkipFinalSnapshot = boolPointer(true)
					})

					It("sets the tag to the plan value by default", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())

						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(input).ToNot(BeNil())

						tagsByName := awsrds.RDSTagsValues(input.Tags)
						Expect(tagsByName).To(HaveKeyWithValue("SkipFinalSnapshot", "true"))
					})

					It("allows the user to override this", func() {
						provisionDetails.RawParameters = json.RawMessage(`{"skip_final_snapshot": false}`)

						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())

						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(input).ToNot(BeNil())

						tagsByName := awsrds.RDSTagsValues(input.Tags)
						Expect(tagsByName).To(HaveKeyWithValue("SkipFinalSnapshot", "false"))
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
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal("with-dash-underscore-" + instanceID))
					expectedDBName := "with_dash_underscore_" + strings.Replace(instanceID, "-", "_", -1)
					Expect(aws.StringValue(input.DBName)).To(Equal(expectedDBName))
				})
			})

			Context("when has AllocatedStorage", func() {
				BeforeEach(func() {
					rdsProperties1.AllocatedStorage = int64Pointer(100)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.Int64Value(input.AllocatedStorage)).To(Equal(int64(100)))
				})
			})

			Context("when has AutoMinorVersionUpgrade", func() {
				BeforeEach(func() {
					rdsProperties1.AutoMinorVersionUpgrade = boolPointer(true)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.BoolValue(input.AutoMinorVersionUpgrade)).To(BeTrue())
				})
			})

			Context("when has AvailabilityZone", func() {
				BeforeEach(func() {
					rdsProperties1.AvailabilityZone = stringPointer("test-az")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.AvailabilityZone)).To(Equal("test-az"))
				})
			})

			Context("when has BackupRetentionPeriod", func() {
				BeforeEach(func() {
					rdsProperties1.BackupRetentionPeriod = int64Pointer(7)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.Int64Value(input.BackupRetentionPeriod)).To(Equal(int64(7)))
				})

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has BackupRetentionPeriod Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"backup_retention_period": 12}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(aws.Int64Value(input.BackupRetentionPeriod)).To(Equal(int64(12)))
					})
				})
			})

			Context("when has default BackupRetentionPeriod", func() {
				It("has backups turned off", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.Int64Value(input.BackupRetentionPeriod)).To(Equal(int64(0)))
				})
			})

			Context("when has CharacterSetName", func() {
				BeforeEach(func() {
					rdsProperties1.CharacterSetName = stringPointer("test-characterset-name")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.CharacterSetName)).To(Equal("test-characterset-name"))
				})

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has CharacterSetName Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"character_set_name": "test-characterset-name-parameter"}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(aws.StringValue(input.CharacterSetName)).To(Equal("test-characterset-name-parameter"))
					})
				})
			})

			Context("when has CopyTagsToSnapshot", func() {
				BeforeEach(func() {
					rdsProperties1.CopyTagsToSnapshot = boolPointer(true)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.BoolValue(input.CopyTagsToSnapshot)).To(BeTrue())
				})
			})

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("when has DBName parameter", func() {
				BeforeEach(func() {
					provisionDetails.RawParameters = json.RawMessage(`{"dbname": "test-dbname"}`)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.DBName)).To(Equal("test-dbname"))
				})
			})

			Context("when has DBSecurityGroups", func() {
				BeforeEach(func() {
					rdsProperties1.DBSecurityGroups = []*string{stringPointer("test-db-security-group")}
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(input.DBSecurityGroups).To(Equal(
						[]*string{aws.String("test-db-security-group")},
					))
				})
			})

			Context("when has DBSubnetGroupName", func() {
				BeforeEach(func() {
					rdsProperties1.DBSubnetGroupName = stringPointer("test-db-subnet-group-name")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.DBSubnetGroupName)).To(Equal("test-db-subnet-group-name"))
				})
			})

			Context("when has EngineVersion", func() {
				BeforeEach(func() {
					rdsProperties1.EngineVersion = stringPointer("1.2.3")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.EngineVersion)).To(Equal("1.2.3"))
				})
			})

			Context("when has Iops", func() {
				BeforeEach(func() {
					rdsProperties1.Iops = int64Pointer(1000)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.Int64Value(input.Iops)).To(Equal(int64(1000)))
				})
			})

			Context("when has KmsKeyID", func() {
				BeforeEach(func() {
					rdsProperties1.KmsKeyID = stringPointer("test-kms-key-id")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.KmsKeyId)).To(Equal("test-kms-key-id"))
				})
			})

			Context("when has LicenseModel", func() {
				BeforeEach(func() {
					rdsProperties1.LicenseModel = stringPointer("test-license-model")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.LicenseModel)).To(Equal("test-license-model"))
				})
			})

			Context("when has MultiAZ", func() {
				BeforeEach(func() {
					rdsProperties1.MultiAZ = boolPointer(true)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.BoolValue(input.MultiAZ)).To(BeTrue())
				})
			})

			Context("when has OptionGroupName", func() {
				BeforeEach(func() {
					rdsProperties1.OptionGroupName = stringPointer("test-option-group-name")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.OptionGroupName)).To(Equal("test-option-group-name"))
				})
			})

			Context("when has Port", func() {
				BeforeEach(func() {
					rdsProperties1.Port = int64Pointer(3306)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.Int64Value(input.Port)).To(Equal(int64(3306)))
				})
			})

			Context("when has PreferredBackupWindow", func() {
				BeforeEach(func() {
					rdsProperties1.PreferredBackupWindow = stringPointer("test-preferred-backup-window")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.PreferredBackupWindow)).To(Equal("test-preferred-backup-window"))
				})

				Context("but has PreferredBackupWindow Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"preferred_backup_window": "test-preferred-backup-window-parameter"}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(aws.StringValue(input.PreferredBackupWindow)).To(Equal("test-preferred-backup-window-parameter"))
					})
				})
			})

			Context("when has PreferredMaintenanceWindow", func() {
				BeforeEach(func() {
					rdsProperties1.PreferredMaintenanceWindow = stringPointer("test-preferred-maintenance-window")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.PreferredMaintenanceWindow)).To(Equal("test-preferred-maintenance-window"))
				})

				Context("but has PreferredMaintenanceWindow Parameter", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"preferred_maintenance_window": "test-preferred-maintenance-window-parameter"}`)
					})

					It("makes the proper calls", func() {
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(aws.StringValue(input.PreferredMaintenanceWindow)).To(Equal("test-preferred-maintenance-window-parameter"))
					})
				})
			})

			Context("when has PubliclyAccessible", func() {
				BeforeEach(func() {
					rdsProperties1.PubliclyAccessible = boolPointer(true)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.BoolValue(input.PubliclyAccessible)).To(BeTrue())
				})
			})

			Context("when has StorageEncrypted", func() {
				BeforeEach(func() {
					rdsProperties1.StorageEncrypted = boolPointer(true)
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.BoolValue(input.StorageEncrypted)).To(BeTrue())
				})
			})

			Context("when has StorageType", func() {
				BeforeEach(func() {
					rdsProperties1.StorageType = stringPointer("test-storage-type")
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
					Expect(aws.StringValue(input.StorageType)).To(Equal("test-storage-type"))
				})
			})

			Context("when has VpcSecurityGroupIds", func() {
				BeforeEach(func() {
					rdsProperties1.VpcSecurityGroupIds = []*string{stringPointer("test-vpc-security-group-ids")}
				})

				It("makes the proper calls", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))
					input := rdsInstance.CreateArgsForCall(0)
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
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
				})
			})

			Context("when Parameters are not valid", func() {

				It("returns an error", func() {
					provisionDetails.RawParameters = json.RawMessage(`not JSON`)
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(0))
				})

				Context("and user provision parameters are not allowed", func() {
					BeforeEach(func() {
						allowUserProvisionParameters = false
					})

					It("does not return an error", func() {
						provisionDetails.RawParameters = json.RawMessage(`not JSON`)
						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())
					})
				})

				It("returns an error for extra params", func() {
					provisionDetails.RawParameters = json.RawMessage(`{"foo": "bar"}`)
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(MatchError(ContainSubstring(`unknown field "foo"`)))
					Expect(rdsInstance.CreateCallCount()).To(Equal(0))
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
					rdsInstance.CreateReturns(errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("operation failed"))
				})
			})

			Context("when using a postgres plan", func() {
				BeforeEach(func() {
					provisionDetails.PlanID = "Plan-3"
					provisionDetails.ServiceID = "Service-3"
				})

				It("will enable the plan's default extensions when no other extensions have been requested", func() {
					payload := []byte(`{"enable_extensions": []}`)
					payloadMessage := (*json.RawMessage)(&payload)
					provisionDetails.RawParameters = *payloadMessage

					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))

					input := rdsInstance.CreateArgsForCall(0)
					tags := awsrds.RDSTagsValues(input.Tags)
					extensionsTag, exists := tags["Extensions"]
					Expect(exists).To(BeTrue())
					Expect(extensionsTag).ToNot(BeEmpty())

					for _, ext := range plan1.RDSProperties.DefaultExtensions {
						Expect(extensionsTag).To(ContainSubstring(aws.StringValue(ext)))
					}
				})

				It("will enable the plan's default extensions in addition to any extensions requested", func() {
					payload := []byte(`{"enable_extensions": ["postgres_super_extension"]}`)
					payloadMessage := (*json.RawMessage)(&payload)
					provisionDetails.RawParameters = *payloadMessage

					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.CreateCallCount()).To(Equal(1))

					input := rdsInstance.CreateArgsForCall(0)
					tags := awsrds.RDSTagsValues(input.Tags)
					extensionsTag, exists := tags["Extensions"]
					Expect(exists).To(BeTrue())
					Expect(extensionsTag).ToNot(BeEmpty())

					Expect(extensionsTag).To(ContainSubstring("postgres_super_extension"))
					for _, ext := range plan1.RDSProperties.DefaultExtensions {
						Expect(extensionsTag).To(ContainSubstring(aws.StringValue(ext)))
					}
				})

				It("returns an error when an extension isn't supported", func() {
					jsonData := []byte(`{"enable_extensions": ["foo"]}`)
					rawparams := (*json.RawMessage)(&jsonData)
					provisionDetails.RawParameters = *rawparams

					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).To(HaveOccurred())
				})

				It("doesn't return an error when an extension isn't provided", func() {
					jsonData := []byte(`{}`)
					rawparams := (*json.RawMessage)(&jsonData)
					provisionDetails.RawParameters = *rawparams
					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
					Expect(err).NotTo(HaveOccurred())
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
			Expect(err).ToNot(HaveOccurred())
			Expect(rdsInstance.DeleteCallCount()).To(Equal(1))
			id, skipFinalSnapshot := rdsInstance.DeleteArgsForCall(0)
			Expect(id).To(Equal(dbInstanceIdentifier))
			Expect(skipFinalSnapshot).To(BeTrue())
		})

		Context("when it does not skip final snaphot", func() {
			BeforeEach(func() {
				rdsProperties1.SkipFinalSnapshot = boolPointer(false)
			})

			It("makes the proper calls", func() {
				_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.DeleteCallCount()).To(Equal(1))
				id, skipFinalSnapshot := rdsInstance.DeleteArgsForCall(0)
				Expect(id).To(Equal(dbInstanceIdentifier))
				Expect(skipFinalSnapshot).To(BeFalse())
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
				rdsInstance.DeleteReturns(errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Deprovision(ctx, instanceID, deprovisionDetails, acceptsIncomplete)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB instance does not exists", func() {
				BeforeEach(func() {
					rdsInstance.DeleteReturns(awsrds.ErrDBInstanceDoesNotExist)
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

			rdsInstance.DescribeReturns(&rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				Endpoint: &rds.Endpoint{
					Address: aws.String("endpoint-address"),
					Port:    aws.Int64(3306),
				},
				DBName:         aws.String("test-db"),
				MasterUsername: aws.String("master-username"),
			}, nil)

			sqlEngine.CreateUserUsername = dbUsername
			sqlEngine.CreateUserPassword = "secret"
		})

		It("returns the proper response", func() {
			bindingResponse, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
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
			_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
			Expect(rdsInstance.DescribeCallCount()).To(Equal(1))
			Expect(err).ToNot(HaveOccurred())
			id := rdsInstance.DescribeArgsForCall(0)
			Expect(id).To(Equal(dbInstanceIdentifier))

			Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
			Expect(sqlProvider.GetSQLEngineEngine).To(Equal("test-engine-one"))
			Expect(sqlEngine.OpenCalled).To(BeTrue())
			Expect(sqlEngine.OpenAddress).To(Equal("endpoint-address"))
			Expect(sqlEngine.OpenPort).To(Equal(int64(3306)))
			Expect(sqlEngine.OpenDBName).To(Equal("test-db"))
			Expect(sqlEngine.OpenUsername).To(Equal("master-username"))
			Expect(sqlEngine.OpenPassword).ToNot(BeEmpty())
			Expect(sqlEngine.CreateUserCalled).To(BeTrue())
			Expect(sqlEngine.CreateUserBindingID).To(Equal(bindingID))
			Expect(sqlEngine.CreateUserDBName).To(Equal("test-db"))
			Expect(sqlEngine.CreateUserReadOnly).To(Equal(false))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
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
			req.Header.Set("X-Broker-API-Version", "2.14")
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
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when present as an empty JSON document", func() {
				BeforeEach(func() {
					bindDetails.RawParameters = json.RawMessage("{}")
				})

				It("does not return an error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when creating a read only binding", func() {
				BeforeEach(func() {
					bindDetails.RawParameters = json.RawMessage(`{"read_only": true}`)
				})

				Context("when the engine is postgres", func() {
					BeforeEach(func() {
						rdsInstance.DescribeReturns(&rds.DBInstance{
							DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
							Endpoint: &rds.Endpoint{
								Address: aws.String("endpoint-address"),
								Port:    aws.Int64(3306),
							},
							DBName:         aws.String("test-db"),
							MasterUsername: aws.String("master-username"),
							Engine:         aws.String("postgres"),
						}, nil)
					})

					It("creates a read only binding successfully", func() {
						_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
						Expect(err).ToNot(HaveOccurred())

						Expect(sqlEngine.CreateUserReadOnly).To(Equal(true))
					})
				})

				It("creates returns an error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
					Expect(err).To(MatchError(ContainSubstring(
						"Read only bindings are only supported for postgres",
					)))
				})
			})
		})

		Context("when Parameters are not valid", func() {

			It("returns the proper error", func() {
				bindDetails.RawParameters = json.RawMessage(`not JSON`)
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(sqlProvider.GetSQLEngineCalled).To(BeFalse())
			})

			Context("and user bind parameters are not allowed", func() {
				BeforeEach(func() {
					allowUserBindParameters = false
				})

				It("does not return an error", func() {
					bindDetails.RawParameters = json.RawMessage(`not JSON`)
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			It("returns an error for extra params", func() {
				bindDetails.RawParameters = json.RawMessage(`{"foo": "bar"}`)
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(MatchError(ContainSubstring(`unknown field "foo"`)))
				Expect(sqlProvider.GetSQLEngineCalled).To(BeFalse())
			})
		})

		Context("when Service is not found", func() {
			BeforeEach(func() {
				bindDetails.ServiceID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service 'unknown' not found"))
			})
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				bindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				rdsInstance.DescribeReturns(nil, errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					rdsInstance.DescribeReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
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
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Engine 'unknown' not supported"))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
			})
		})

		Context("when creating a DB user fails", func() {
			BeforeEach(func() {
				sqlEngine.CreateUserError = errors.New("Failed to create user")
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
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

			rdsInstance.DescribeReturns(&rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				Endpoint: &rds.Endpoint{
					Address: aws.String("endpoint-address"),
					Port:    aws.Int64(3306),
				},
				DBName:         aws.String("test-db"),
				MasterUsername: aws.String("master-username"),
				Engine:         aws.String("test-engine-one"),
			}, nil)
		})

		It("makes the proper calls", func() {
			spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)

			Expect(rdsInstance.DescribeCallCount()).To(Equal(1))
			Expect(err).ToNot(HaveOccurred())
			id := rdsInstance.DescribeArgsForCall(0)
			Expect(id).To(Equal(dbInstanceIdentifier))
			Expect(spec.OperationData).To(Equal(""))

			Expect(sqlProvider.GetSQLEngineCalled).To(BeTrue())
			Expect(sqlProvider.GetSQLEngineEngine).To(Equal("test-engine-one"))
			Expect(sqlEngine.OpenCalled).To(BeTrue())
			Expect(sqlEngine.OpenAddress).To(Equal("endpoint-address"))
			Expect(sqlEngine.OpenPort).To(Equal(int64(3306)))
			Expect(sqlEngine.OpenDBName).To(Equal("test-db"))
			Expect(sqlEngine.OpenUsername).To(Equal("master-username"))
			Expect(sqlEngine.OpenPassword).ToNot(BeEmpty())
			Expect(sqlEngine.DropUserCalled).To(BeTrue())
			Expect(sqlEngine.DropUserBindingID).To(Equal(bindingID))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
		})

		Context("when Service Plan is not found", func() {
			BeforeEach(func() {
				unbindDetails.PlanID = "unknown"
			})

			It("returns the proper error", func() {
				spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Service Plan 'unknown' not found"))
				Expect(spec.OperationData).To(Equal(""))
			})
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				rdsInstance.DescribeReturns(nil, errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
				Expect(spec.OperationData).To(Equal(""))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					rdsInstance.DescribeReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
				})

				It("returns the proper error", func() {
					spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
					Expect(spec.OperationData).To(Equal(""))
				})
			})
		})

		Context("when getting the SQL Engine fails", func() {
			BeforeEach(func() {
				sqlProvider.GetSQLEngineError = errors.New("SQL Engine 'unknown' not supported")
			})

			It("returns the proper error", func() {
				spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("SQL Engine 'unknown' not supported"))
				Expect(spec.OperationData).To(Equal(""))
			})
		})

		Context("when opening a DB connection fails", func() {
			BeforeEach(func() {
				sqlEngine.OpenError = errors.New("Failed to open sqlEngine")
			})

			It("returns the proper error", func() {
				spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to open sqlEngine"))
				Expect(spec.OperationData).To(Equal(""))
			})
		})

		Context("when deleting a user fails", func() {
			BeforeEach(func() {
				sqlEngine.DropUserError = errors.New("Failed to delete user")
			})

			It("returns the proper error", func() {
				spec, err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails, false)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Failed to delete user"))
				Expect(sqlEngine.CloseCalled).To(BeTrue())
				Expect(spec.OperationData).To(Equal(""))
			})
		})
	})

	Describe("LastOperation", func() {
		var (
			dbInstanceStatus            string
			lastOperationState          brokerapi.LastOperationState
			properLastOperationResponse brokerapi.LastOperation
			parameterGroupStatus        string
			dbAllocatedStorage          int64

			defaultDBInstance = &rds.DBInstance{
				AllocatedStorage:     int64Pointer(300),
				DBInstanceClass:      stringPointer("db.m3.test"),
				MultiAZ:              boolPointer(false),
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceArn:        aws.String(dbInstanceArn),
				Engine:               aws.String("test-engine"),
				EngineVersion:        stringPointer("4.8.9"),
				Endpoint: &rds.Endpoint{
					Address: aws.String("endpoint-address"),
					Port:    aws.Int64(3306),
				},
				DBName:         aws.String("test-db"),
				MasterUsername: aws.String("master-username"),
				DBParameterGroups: []*rds.DBParameterGroupStatus{
					&rds.DBParameterGroupStatus{
						DBParameterGroupName: aws.String("rdsbroker-testengine10"),
					},
				},
			}

			defaultDBInstanceTagsByName = map[string]string{
				"Owner": "Cloud Foundry",
				"Broker Name": "mybroker",
				"Created by": "AWS RDS Service Broker",
				"Service ID": "Service-3",
				"Plan ID": "Plan-3",
				"Extensions": "postgis:pg-stat-statements",
			}

			pollDetails = brokerapi.PollDetails{
				ServiceID: "Service-3",
				PlanID: "Plan-3",
				OperationData: "123blah",
			}
		)

		BeforeEach(func() {
			parameterGroupStatus = "in-sync"
			dbAllocatedStorage = 300
		})

		JustBeforeEach(func() {
			defaultDBInstance.DBInstanceStatus = aws.String(dbInstanceStatus)
			defaultDBInstance.DBParameterGroups[0].ParameterApplyStatus = aws.String(parameterGroupStatus)
			defaultDBInstance.AllocatedStorage = int64Pointer(dbAllocatedStorage)
			rdsInstance.DescribeReturns(defaultDBInstance, nil)

			rdsInstance.GetResourceTagsReturns(
				awsrds.BuildRDSTags(defaultDBInstanceTagsByName),
				nil,
			)

			rdsInstance.ModifyReturns(
				&rds.DBInstance{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
					DBInstanceArn:        aws.String(dbInstanceArn),
				},
				nil,
			)

			properLastOperationResponse = brokerapi.LastOperation{
				State:       lastOperationState,
				Description: "DB Instance '" + dbInstanceIdentifier + "' status is '" + dbInstanceStatus + "'",
			}
		})

		Context("when describing the DB Instance fails", func() {
			JustBeforeEach(func() {
				rdsInstance.DescribeReturns(nil, errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				JustBeforeEach(func() {
					rdsInstance.DescribeReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
				})

				It("returns the proper error", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
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
					req.Header.Set("X-Broker-API-Version", "2.14")
					req.SetBasicAuth(brokeruser, brokerpass)
					fmt.Fprintf(GinkgoWriter, "%s:\n", recorder.Body.Bytes())

					rdsBrokerServer.ServeHTTP(recorder, req)
					Expect(recorder.Code).To(Equal(410))
				})

			})
		})

		It("returns InstanceDoesNotExist if it is not found when getting the tags", func() {
			rdsInstance.GetResourceTagsReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
			_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
		})

		Context("when last operation is still in progress", func() {
			BeforeEach(func() {
				dbInstanceStatus = "creating"
				lastOperationState = brokerapi.InProgress
			})

			It("calls GetResourceTags() with the refresh cache option", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))

				Expect(rdsInstance.GetResourceTagsCallCount()).To(Equal(1))
				id, opts := rdsInstance.GetResourceTagsArgsForCall(0)
				Expect(id).To(Equal(dbInstanceArn))

				Expect(opts).To(ContainElement(awsrds.DescribeRefreshCacheOption))
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("and there are pending post restore tasks", func() {
				JustBeforeEach(func() {
					newDBInstanceTagsByName := copyStringStringMap(defaultDBInstanceTagsByName)
					newDBInstanceTagsByName["PendingUpdateSettings"] = "true"
					rdsInstance.GetResourceTagsReturns(
						awsrds.BuildRDSTags(newDBInstanceTagsByName),
						nil,
					)
				})
				It("should not call RemoveTag to remove the tag PendingUpdateSettings", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(0))
				})

				It("should not modify the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
				})
			})
		})

		Context("when the parameter group has a pending-reboot state", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				parameterGroupStatus = "pending-reboot"
			})

			It("reboots the database", func() {
				lastOperationState, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationState.State).To(Equal(brokerapi.InProgress))
				Expect(rdsInstance.RebootCallCount()).To(Equal(1))
			})
		})

		Context("when the parameter group has a pending-reboot state and instance is unavailable", func() {
			BeforeEach(func() {
				dbInstanceStatus = "modifying"
				parameterGroupStatus = "pending-reboot"
			})

			It("reboots the database", func() {
				lastOperationState, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationState.State).To(Equal(brokerapi.InProgress))
				Expect(rdsInstance.RebootCallCount()).To(Equal(0))
			})
		})

		Context("when last operation failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "failed"
				lastOperationState = brokerapi.Failed
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})
		})

		Context("when a simple major version upgrade failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
			})

			JustBeforeEach(func() {
				newDBInstanceTagsByName := copyStringStringMap(defaultDBInstanceTagsByName)
				newDBInstanceTagsByName["Plan ID"] = "Plan-4"
				rdsInstance.GetResourceTagsReturns(
					awsrds.BuildRDSTags(newDBInstanceTagsByName),
					nil,
				)
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(brokerapi.LastOperation{
					State: brokerapi.Failed,
					Description: "Plan upgrade failed. Refer to database logs for more information.",
				}))
			})

			It("rolls back the Plan ID tag to match reality", func() {
				_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))

				id, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				Expect(id).To(Equal(dbInstanceArn))
				tagsByName := awsrds.RDSTagsValues(tags)

				Expect(tagsByName).To(Equal(defaultDBInstanceTagsByName))
			})
		})

		Context("when a complex major version upgrade failed", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				dbAllocatedStorage = 400
			})

			JustBeforeEach(func() {
				newDBInstanceTagsByName := copyStringStringMap(defaultDBInstanceTagsByName)
				newDBInstanceTagsByName["Plan ID"] = "Plan-5"
				rdsInstance.GetResourceTagsReturns(
					awsrds.BuildRDSTags(newDBInstanceTagsByName),
					nil,
				)
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(brokerapi.LastOperation{
					State: brokerapi.Failed,
					Description: "Operation failed and will need manual intervention to resolve. Please contact support.",
				}))
			})

			It("rolls back the Plan ID tag to match reality", func() {
				_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))

				id, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
				Expect(id).To(Equal(dbInstanceArn))
				tagsByName := awsrds.RDSTagsValues(tags)

				Expect(tagsByName).To(Equal(defaultDBInstanceTagsByName))
			})
		})

		Context("when last operation succeeded", func() {
			BeforeEach(func() {
				dbInstanceStatus = "available"
				lastOperationState = brokerapi.Succeeded
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("the SQL engine is Postgres", func() {
				JustBeforeEach(func() {
					defaultDBInstance.Engine = aws.String("postgres")
					rdsInstance.DescribeReturns(defaultDBInstance, nil)
				})

				It("attempts to create Postgres extenions", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.CreateExtensionsCalled).To(BeTrue())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})

			Context("but has pending modifications", func() {
				JustBeforeEach(func() {
					newDBInstance := *defaultDBInstance
					newDBInstance.PendingModifiedValues = &rds.PendingModifiedValues{
						MasterUserPassword: aws.String("foo"),
					}
					rdsInstance.DescribeReturns(&newDBInstance, nil)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending modifications",
					}
				})

				It("returns the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})

			It("If instance.PendingModifiedValues is empty it returns the right state", func() {
				newDBInstance := *defaultDBInstance
				newDBInstance.PendingModifiedValues = &rds.PendingModifiedValues{}
				rdsInstance.DescribeReturns(&newDBInstance, nil)
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("but there are pending post restore tasks", func() {
				JustBeforeEach(func() {
					newDBInstanceTagsByName := copyStringStringMap(defaultDBInstanceTagsByName)
					newDBInstanceTagsByName["PendingUpdateSettings"] = "true"
					rdsInstance.GetResourceTagsReturns(
						awsrds.BuildRDSTags(newDBInstanceTagsByName),
						nil,
					)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})
				It("should call RemoveTag to remove the tag PendingUpdateSettings", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(1))
					id, tagName := rdsInstance.RemoveTagArgsForCall(0)
					Expect(id).To(Equal(dbInstanceIdentifier))
					Expect(tagName).To(Equal("PendingUpdateSettings"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						rdsInstance.RemoveTagReturns(errors.New("Failed to remove tag"))
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to remove tag"))
					})
				})

				It("modifies the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
					input := rdsInstance.ModifyArgsForCall(0)
					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				})

				It("sets the right tags", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())

					Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
					id, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
					Expect(id).To(Equal(dbInstanceArn))
					tagsByName := awsrds.RDSTagsValues(tags)

					Expect(tagsByName).To(HaveKeyWithValue("Owner", "Cloud Foundry"))
					Expect(tagsByName).To(HaveKeyWithValue("Broker Name", "mybroker"))
					Expect(tagsByName).To(HaveKeyWithValue("Restored by", "AWS RDS Service Broker"))
					Expect(tagsByName).To(HaveKey("Restored at"))
					Expect(tagsByName).To(HaveKeyWithValue("Service ID", "Service-3"))
					Expect(tagsByName).To(HaveKeyWithValue("Plan ID", "Plan-3"))
					Expect(tagsByName).To(HaveKeyWithValue("chargeable_entity", instanceID))
				})

				Context("when the master password needs to be rotated", func() {
					JustBeforeEach(func() {
						sqlEngine.CorrectPassword = "some-other-password"
						// use a stub function to set the password back to what it was before. This is because the Bind()
						// uses two different calls to the SQL engine's Open() method, the first needs to fail and the second needs to pass.
						rdsInstance.ModifyStub = func(input *rds.ModifyDBInstanceInput) (*rds.DBInstance, error) {
							sqlEngine.CorrectPassword = aws.StringValue(input.MasterUserPassword)
							return &rds.DBInstance{DBInstanceIdentifier: input.DBInstanceIdentifier}, nil
						}
					})

					It("should try to change the master password", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
						input := rdsInstance.ModifyArgsForCall(0)
						Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
						Expect(aws.StringValue(input.MasterUserPassword)).ToNot(BeEmpty())
					})
				})

				Context("when has DBSecurityGroups", func() {
					BeforeEach(func() {
						rdsProperties3.DBSecurityGroups = []*string{aws.String("test-db-security-group")}
					})

					It("makes the modify with the security group", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
						input := rdsInstance.ModifyArgsForCall(0)
						Expect(input.DBSecurityGroups).To(Equal(
							[]*string{aws.String("test-db-security-group")},
						))
					})
				})
			})

			Context("but there are pending reboot", func() {
				JustBeforeEach(func() {
					newDBInstanceTagsByName := copyStringStringMap(defaultDBInstanceTagsByName)
					newDBInstanceTagsByName["PendingReboot"] = "true"
					rdsInstance.GetResourceTagsReturns(
						awsrds.BuildRDSTags(newDBInstanceTagsByName),
						nil,
					)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})

				It("should call RemoveTag to remove the tag PendingReboot", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(1))
					id, tagName := rdsInstance.RemoveTagArgsForCall(0)
					Expect(id).To(Equal(dbInstanceIdentifier))
					Expect(tagName).To(Equal("PendingReboot"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						rdsInstance.RemoveTagReturns(errors.New("Failed to remove tag"))
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to remove tag"))
					})
				})

				It("reboot the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RebootCallCount()).To(Equal(1))
					input := rdsInstance.RebootArgsForCall(0)
					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				})
			})

			Context("but there is a pending reset user password", func() {
				JustBeforeEach(func() {
					newDBInstanceTagsByName := copyStringStringMap(defaultDBInstanceTagsByName)
					newDBInstanceTagsByName["PendingResetUserPassword"] = "true"
					rdsInstance.GetResourceTagsReturns(
						awsrds.BuildRDSTags(newDBInstanceTagsByName),
						nil,
					)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})

				It("should call RemoveTag to remove the tag PendingResetUserPassword", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(1))
					id, tagName := rdsInstance.RemoveTagArgsForCall(0)
					Expect(id).To(Equal(dbInstanceIdentifier))
					Expect(tagName).To(Equal("PendingResetUserPassword"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						rdsInstance.RemoveTagReturns(errors.New("Failed to remove tag"))
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to remove tag"))
					})
				})

				It("should reset the database state by calling sqlengine.ResetState()", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.ResetStateCalled).To(BeTrue())
				})

				Context("when sqlengine.ResetState() fails", func() {
					BeforeEach(func() {
						sqlEngine.ResetStateError = errors.New("Failed to reset state")
					})
					It("returns the proper error", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(Equal("Failed to reset state"))
					})
				})
			})

			Context("but there are not post restore tasks or reset password to execute", func() {
				It("should not try to change the master password", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
				})
				It("should not reset the database state by not calling sqlengine.ResetState()", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.ResetStateCalled).To(BeFalse())
				})
				It("should not call RemoveTag", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(0))
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
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, pollDetails)
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
			"storage-optimization",
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
		BeforeEach(func() {
			sqlEngine = &sqlfake.FakeSQLEngine{}
			sqlProvider.GetSQLEngineSQLEngine = sqlEngine
		})

		Context("when there is no DB instance", func() {
			It("shouldn't try to connect to databases", func() {
				rdsBroker.CheckAndRotateCredentials()
				Expect(sqlProvider.GetSQLEngineCalled).To(BeFalse())
				Expect(sqlEngine.OpenCalled).To(BeFalse())
			})
		})

		Context("when there are DB instances", func() {
			BeforeEach(func() {
				rdsInstance.DescribeByTagReturns([]*rds.DBInstance{{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
					Endpoint: &rds.Endpoint{
						Address: aws.String("endpoint-address"),
						Port:    aws.Int64(3306),
					},
					DBName:         aws.String("test-db"),
					MasterUsername: aws.String("master-username"),
					Engine:         aws.String("fake-engine"),
				}}, nil)
			})

			It("should try to connect to databases", func() {
				rdsBroker.CheckAndRotateCredentials()
				Expect(rdsInstance.DescribeByTagCallCount()).To(Equal(1))
				key, value, opts := rdsInstance.DescribeByTagArgsForCall(0)
				Expect(key).To(BeEquivalentTo("Broker Name"))
				Expect(value).To(BeEquivalentTo(brokerName))
				Expect(opts).To(BeEmpty())

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
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
				})
			})

			Context("and the passwords don't work", func() {
				BeforeEach(func() {
					sqlEngine.OpenError = sqlengine.LoginFailedError
				})

				It("should try to change the master password", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
					input := rdsInstance.ModifyArgsForCall(0)

					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(BeEquivalentTo(dbInstanceIdentifier))
					Expect(aws.StringValue(input.MasterUserPassword)).To(BeEquivalentTo(sqlEngine.OpenPassword))
				})
			})

			Context("and there is an unkown open error", func() {
				BeforeEach(func() {
					sqlEngine.OpenError = errors.New("Unknown open connection error")
				})

				It("should not try to change the master password", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
				})
			})

			Context("and there is DescribeByTagError error", func() {
				BeforeEach(func() {
					rdsInstance.DescribeByTagReturns(nil, errors.New("Error when listing instances"))
				})

				It("should not call modify", func() {
					rdsBroker.CheckAndRotateCredentials()
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
				})
			})

		})

		Context("when we reset the password then try to bind", func() {
			var (
				bindDetails brokerapi.BindDetails
			)

			BeforeEach(func() {
				dbInstance := &rds.DBInstance{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
					Endpoint: &rds.Endpoint{
						Address: aws.String("endpoint-address"),
						Port:    aws.Int64(3306),
					},
					DBName:         aws.String("test-db"),
					MasterUsername: aws.String("master-username"),
					Engine:         aws.String("fake-engine"),
				}
				rdsInstance.DescribeByTagReturns([]*rds.DBInstance{dbInstance}, nil)
				rdsInstance.DescribeReturns(dbInstance, nil)

				bindDetails = brokerapi.BindDetails{
					ServiceID:     "Service-3",
					PlanID:        "Plan-3",
					AppGUID:       "Application-1",
					RawParameters: json.RawMessage("{}"),
				}
			})

			It("the new password and the password used in bind are the same", func() {
				sqlEngine.OpenError = sqlengine.LoginFailedError
				rdsBroker.CheckAndRotateCredentials()
				expectedMasterPassword := sqlEngine.OpenPassword

				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.MasterUserPassword)).To(BeEquivalentTo(expectedMasterPassword))

				sqlEngine.OpenError = nil
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails, false)
				Expect(err).ToNot(HaveOccurred())
				Expect(sqlEngine.OpenCalled).To(BeTrue())

				Expect(sqlEngine.OpenPassword).To(BeEquivalentTo(expectedMasterPassword))
			})
		})
	})

})
