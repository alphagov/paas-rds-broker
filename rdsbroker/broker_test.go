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
		plan1          ServicePlan
		plan2          ServicePlan
		plan3          ServicePlan
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
				restoreFromSnapshotInstanceGUID  string
				restoreFromSnapshotDBInstanceID  string
				restoreFromSnapshotDBSnapshotArn string
				dbSnapshotTags                   map[string]string
			)

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
						SnapshotCreateTime:   aws.Time(time.Now()),
					},
				}, nil)

				rdsInstance.GetResourceTagsReturns(awsrds.BuilRDSTags(dbSnapshotTags), nil)
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
				Expect(tagsByName["Owner"]).To(Equal("Cloud Foundry"))
				Expect(tagsByName["Restored by"]).To(Equal("AWS RDS Service Broker"))
				Expect(tagsByName).To(HaveKey("Restored at"))
				Expect(tagsByName["Service ID"]).To(Equal("Service-1"))
				Expect(tagsByName["Plan ID"]).To(Equal("Plan-1"))
				Expect(tagsByName["Organization ID"]).To(Equal("organization-id"))
				Expect(tagsByName["Space ID"]).To(Equal("space-id"))
				Expect(tagsByName["Restored From Snapshot"]).To(Equal(restoreFromSnapshotDBInstanceID + "-1"))
				Expect(tagsByName["PendingResetUserPassword"]).To(Equal("true"))
				Expect(tagsByName["PendingUpdateSettings"]).To(Equal("true"))
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
					rdsInstance.GetResourceTagsReturns(awsrds.BuilRDSTags(dbSnapshotTags), nil)

					_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)

					Expect(err).ToNot(HaveOccurred())

					Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
					_, inputProvisionParameters := paramGroupSelector.SelectParameterGroupArgsForCall(0)
					Expect(inputProvisionParameters.Extensions).To(ContainElement("foo"))
					Expect(inputProvisionParameters.Extensions).To(ContainElement("bar"))
				})

				Context("when the user passes extensions to set", func() {
					BeforeEach(func() {
						provisionDetails.RawParameters = json.RawMessage(`{"restore_from_latest_snapshot_of": "` + restoreFromSnapshotInstanceGUID + `", "enabled_extensions": ["postgres_super_extension"]}`)
					})
					It("adds those extensions to the set of extensions on the snapshot", func() {
						dbSnapshotTags[awsrds.TagExtensions] = "foo:bar"
						rdsInstance.GetResourceTagsReturns(awsrds.BuilRDSTags(dbSnapshotTags), nil)

						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)

						Expect(err).ToNot(HaveOccurred())

						Expect(paramGroupSelector.SelectParameterGroupCallCount()).To(Equal(1))
						_, inputProvisionParameters := paramGroupSelector.SelectParameterGroupArgsForCall(0)
						Expect(inputProvisionParameters.Extensions).To(ContainElement("foo"))
						Expect(inputProvisionParameters.Extensions).To(ContainElement("bar"))
						Expect(inputProvisionParameters.Extensions).To(ContainElement("postgres_super_extension"))
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
				jsonData := []byte(`{"enabled_extensions": ["postgis", "pg_stat_statements"]}`)
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

				Expect(tagsByName["Owner"]).To(Equal("Cloud Foundry"))
				Expect(tagsByName["Created by"]).To(Equal("AWS RDS Service Broker"))
				Expect(tagsByName).To(HaveKey("Created at"))
				Expect(tagsByName["Service ID"]).To(Equal("Service-3"))
				Expect(tagsByName["Plan ID"]).To(Equal("Plan-3"))
				Expect(tagsByName["Organization ID"]).To(Equal("organization-id"))
				Expect(tagsByName["Space ID"]).To(Equal("space-id"))
				Expect(tagsByName["Extensions"]).To(Equal("postgis:pg_stat_statements"))
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
						Expect(tagsByName["SkipFinalSnapshot"]).To(Equal("false"))
					})

					It("allows the user to override this", func() {
						provisionDetails.RawParameters = json.RawMessage(`{"skip_final_snapshot": true}`)

						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())

						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(input).ToNot(BeNil())

						tagsByName := awsrds.RDSTagsValues(input.Tags)
						Expect(tagsByName["SkipFinalSnapshot"]).To(Equal("true"))
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
						Expect(tagsByName["SkipFinalSnapshot"]).To(Equal("true"))
					})

					It("allows the user to override this", func() {
						provisionDetails.RawParameters = json.RawMessage(`{"skip_final_snapshot": false}`)

						_, err := rdsBroker.Provision(ctx, instanceID, provisionDetails, acceptsIncomplete)
						Expect(err).ToNot(HaveOccurred())

						Expect(rdsInstance.CreateCallCount()).To(Equal(1))
						input := rdsInstance.CreateArgsForCall(0)
						Expect(input).ToNot(BeNil())

						tagsByName := awsrds.RDSTagsValues(input.Tags)
						Expect(tagsByName["SkipFinalSnapshot"]).To(Equal("false"))
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

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has PreferredBackupWindow Parameter", func() {
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

				//FIXME: These tests are pending until we allow this user provided parameter
				PContext("but has PreferredMaintenanceWindow Parameter", func() {
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
					payload := []byte(`{"enabled_extensions": []}`)
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
					payload := []byte(`{"enabled_extensions": ["postgres_super_extension"]}`)
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
					jsonData := []byte(`{"enabled_extensions": ["foo"]}`)
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

	Describe("Reboot", func() {
		var (
			updateDetails           brokerapi.UpdateDetails
			acceptsIncomplete       bool
			properUpdateServiceSpec brokerapi.UpdateServiceSpec
		)

		BeforeEach(func() {
			updateDetails = brokerapi.UpdateDetails{
				ServiceID: "Service-1",
				PlanID:    "Plan-1",
				PreviousValues: brokerapi.PreviousValues{
					PlanID:    "Plan-1",
					ServiceID: "Service-1",
					OrgID:     "organization-id",
					SpaceID:   "space-id",
				},
				RawParameters: json.RawMessage(`{ "reboot": true }`),
			}
			acceptsIncomplete = true
			properUpdateServiceSpec = brokerapi.UpdateServiceSpec{
				IsAsync: true,
			}

			rdsInstance.RebootReturns(
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
			Expect(rdsInstance.RebootCallCount()).To(Equal(1))
			input := rdsInstance.RebootArgsForCall(0)
			Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
			Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
		})

		It("passes the force failover option", func() {
			updateDetails.RawParameters = json.RawMessage(`{ "reboot": true, "force_failover": true }`)
			_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(err).ToNot(HaveOccurred())
			Expect(rdsInstance.RebootCallCount()).To(Equal(1))
			input := rdsInstance.RebootArgsForCall(0)
			Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
			Expect(aws.BoolValue(input.ForceFailover)).To(BeTrue())
			Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
		})

		It("fails if the reboot include a plan change", func() {
			updateDetails.RawParameters = json.RawMessage(`{ "reboot": true, "force_failover": true }`)
			updateDetails.PlanID = "plan-2"
			_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("Invalid to reboot and update plan in the same command"))
			Expect(rdsInstance.RebootCallCount()).To(Equal(0))
			Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
		})
	})

	Describe("Update", func() {
		var (
			updateDetails           brokerapi.UpdateDetails
			acceptsIncomplete       bool
			properUpdateServiceSpec brokerapi.UpdateServiceSpec
			existingDbInstance      *rds.DBInstance
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
				RawParameters: json.RawMessage(`{}`),
			}
			acceptsIncomplete = true
			properUpdateServiceSpec = brokerapi.UpdateServiceSpec{
				IsAsync: true,
			}

			existingDbInstance = &rds.DBInstance{
				DBParameterGroups: []*rds.DBParameterGroupStatus{
					&rds.DBParameterGroupStatus{
						DBParameterGroupName: aws.String("rdsbroker-postgres10-envname"),
					},
				},
			}
			rdsInstance.DescribeReturns(existingDbInstance, nil)

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

			Expect(tagsByName["Owner"]).To(Equal("Cloud Foundry"))
			Expect(tagsByName["Broker Name"]).To(Equal("mybroker"))
			Expect(tagsByName["Updated by"]).To(Equal("AWS RDS Service Broker"))
			Expect(tagsByName).To(HaveKey("Updated at"))
			Expect(tagsByName["Service ID"]).To(Equal("Service-2"))
			Expect(tagsByName["Plan ID"]).To(Equal("Plan-2"))
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

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("but has PreferredBackupWindow Parameter", func() {
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

			//FIXME: These tests are pending until we allow this user provided parameter
			PContext("but has PreferredMaintenanceWindow Parameter", func() {
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
				Expect(tagValues["SkipFinalSnapshot"]).To(Equal("true"))
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
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context("when getting resource tags errors", func() {
			BeforeEach(func() {
				rdsInstance.GetResourceTagsReturns(nil, errors.New("operation failed"))
			})

			It("does not return an error", func() {
				_, err := rdsBroker.Update(ctx, instanceID, updateDetails, acceptsIncomplete)
				Expect(err).ToNot(HaveOccurred())
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
				rdsInstance.DescribeReturns(nil, errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					rdsInstance.DescribeReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
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
			err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)

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
			Expect(sqlEngine.DropUserCalled).To(BeTrue())
			Expect(sqlEngine.DropUserBindingID).To(Equal(bindingID))
			Expect(sqlEngine.CloseCalled).To(BeTrue())
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
				rdsInstance.DescribeReturns(nil, errors.New("operation failed"))
			})

			It("returns the proper error", func() {
				err := rdsBroker.Unbind(ctx, instanceID, bindingID, unbindDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				BeforeEach(func() {
					rdsInstance.DescribeReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
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

			defaultDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceArn:        aws.String(dbInstanceArn),
				Engine:               aws.String("test-engine"),
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

			defaultDBInstanceTags = []*rds.Tag{
				{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")},
				{Key: aws.String("Broker Name"), Value: aws.String("mybroker")},
				{Key: aws.String("Created by"), Value: aws.String("AWS RDS Service Broker")},
				{Key: aws.String("Service ID"), Value: aws.String("Service-1")},
				{Key: aws.String("Plan ID"), Value: aws.String("Plan-1")},
				{Key: aws.String("Extensions"), Value: aws.String("postgis:pg-stat-statements")},
			}
		)
		JustBeforeEach(func() {
			defaultDBInstance.DBInstanceStatus = aws.String(dbInstanceStatus)
			rdsInstance.DescribeReturns(defaultDBInstance, nil)

			rdsInstance.GetResourceTagsReturns(defaultDBInstanceTags, nil)

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
				_, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("when the DB Instance does not exists", func() {
				JustBeforeEach(func() {
					rdsInstance.DescribeReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
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

		It("returns InstanceDoesNotExist if it is not found when getting the tags", func() {
			rdsInstance.GetResourceTagsReturns(nil, awsrds.ErrDBInstanceDoesNotExist)
			_, err := rdsBroker.LastOperation(ctx, instanceID, "")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
		})

		Context("when last operation is still in progress", func() {
			BeforeEach(func() {
				dbInstanceStatus = "creating"
				lastOperationState = brokerapi.InProgress
			})

			It("calls GetResourceTags() with the refresh cache option", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))

				Expect(rdsInstance.GetResourceTagsCallCount()).To(Equal(1))
				id, opts := rdsInstance.GetResourceTagsArgsForCall(0)
				Expect(id).To(Equal(dbInstanceArn))

				Expect(opts).To(ContainElement(awsrds.DescribeRefreshCacheOption))
			})

			It("returns the proper LastOperationResponse", func() {
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("and there are pending post restore tasks", func() {
				JustBeforeEach(func() {
					rdsInstance.GetResourceTagsReturns(
						append(
							defaultDBInstanceTags,
							&rds.Tag{Key: aws.String("PendingUpdateSettings"), Value: aws.String("true")},
						),
						nil,
					)
				})
				It("should not call RemoveTag to remove the tag PendingUpdateSettings", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(0))
				})

				It("should not modify the DB instance", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
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
					defaultDBInstance.Engine = aws.String("postgres")
					rdsInstance.DescribeReturns(defaultDBInstance, nil)
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
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})
			})

			It("If instance.PendingModifiedValues is empty it returns the right state", func() {
				newDBInstance := *defaultDBInstance
				newDBInstance.PendingModifiedValues = &rds.PendingModifiedValues{}
				rdsInstance.DescribeReturns(&newDBInstance, nil)
				lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
				Expect(err).ToNot(HaveOccurred())
				Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
			})

			Context("but there are pending post restore tasks", func() {
				JustBeforeEach(func() {
					rdsInstance.GetResourceTagsReturns(
						append(
							defaultDBInstanceTags,
							&rds.Tag{Key: aws.String("PendingUpdateSettings"), Value: aws.String("true")},
						),
						nil,
					)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})
				It("should call RemoveTag to remove the tag PendingUpdateSettings", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(1))
					id, tagName := rdsInstance.RemoveTagArgsForCall(0)
					Expect(id).To(Equal(dbInstanceIdentifier))
					Expect(tagName).To(Equal("PendingUpdateSettings"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						rdsInstance.RemoveTagReturns(errors.New("Failed to remove tag"))
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
					Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
					input := rdsInstance.ModifyArgsForCall(0)
					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				})

				It("sets the right tags", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())

					Expect(rdsInstance.AddTagsToResourceCallCount()).To(Equal(1))
					id, tags := rdsInstance.AddTagsToResourceArgsForCall(0)
					Expect(id).To(Equal(dbInstanceArn))
					tagsByName := awsrds.RDSTagsValues(tags)

					Expect(tagsByName["Owner"]).To(Equal("Cloud Foundry"))
					Expect(tagsByName["Broker Name"]).To(Equal("mybroker"))
					Expect(tagsByName["Restored by"]).To(Equal("AWS RDS Service Broker"))
					Expect(tagsByName).To(HaveKey("Restored at"))
					Expect(tagsByName["Service ID"]).To(Equal("Service-1"))
					Expect(tagsByName["Plan ID"]).To(Equal("Plan-1"))
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
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
						Expect(err).ToNot(HaveOccurred())
						Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
						input := rdsInstance.ModifyArgsForCall(0)
						Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
						Expect(aws.StringValue(input.MasterUserPassword)).ToNot(BeEmpty())
					})
				})

				Context("when has DBSecurityGroups", func() {
					BeforeEach(func() {
						rdsProperties1.DBSecurityGroups = []*string{aws.String("test-db-security-group")}
					})

					It("makes the modify with the security group", func() {
						_, err := rdsBroker.LastOperation(ctx, instanceID, "")
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
					rdsInstance.GetResourceTagsReturns(
						append(
							defaultDBInstanceTags,
							&rds.Tag{Key: aws.String("PendingReboot"), Value: aws.String("true")},
						),
						nil,
					)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})

				It("should call RemoveTag to remove the tag PendingReboot", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(1))
					id, tagName := rdsInstance.RemoveTagArgsForCall(0)
					Expect(id).To(Equal(dbInstanceIdentifier))
					Expect(tagName).To(Equal("PendingReboot"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						rdsInstance.RemoveTagReturns(errors.New("Failed to remove tag"))
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
					Expect(rdsInstance.RebootCallCount()).To(Equal(1))
					input := rdsInstance.RebootArgsForCall(0)
					Expect(aws.StringValue(input.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
				})
			})

			Context("but there is a pending reset user password", func() {
				JustBeforeEach(func() {
					rdsInstance.GetResourceTagsReturns(
						append(
							defaultDBInstanceTags,
							&rds.Tag{Key: aws.String("PendingResetUserPassword"), Value: aws.String("true")},
						),
						nil,
					)

					properLastOperationResponse = brokerapi.LastOperation{
						State:       brokerapi.InProgress,
						Description: "DB Instance '" + dbInstanceIdentifier + "' has pending post restore modifications",
					}
				})

				It("should call RemoveTag to remove the tag PendingResetUserPassword", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(rdsInstance.RemoveTagCallCount()).To(Equal(1))
					id, tagName := rdsInstance.RemoveTagArgsForCall(0)
					Expect(id).To(Equal(dbInstanceIdentifier))
					Expect(tagName).To(Equal("PendingResetUserPassword"))
				})

				It("should return the proper LastOperationResponse", func() {
					lastOperationResponse, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(lastOperationResponse).To(Equal(properLastOperationResponse))
				})

				Context("when remove tag fails", func() {
					BeforeEach(func() {
						rdsInstance.RemoveTagReturns(errors.New("Failed to remove tag"))
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
					Expect(rdsInstance.ModifyCallCount()).To(Equal(0))
				})
				It("should not reset the database state by not calling sqlengine.ResetState()", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
					Expect(err).ToNot(HaveOccurred())
					Expect(sqlEngine.ResetStateCalled).To(BeFalse())
				})
				It("should not call RemoveTag", func() {
					_, err := rdsBroker.LastOperation(ctx, instanceID, "")
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

				Expect(rdsInstance.ModifyCallCount()).To(Equal(1))
				input := rdsInstance.ModifyArgsForCall(0)
				Expect(aws.StringValue(input.MasterUserPassword)).To(BeEquivalentTo(expectedMasterPassword))

				sqlEngine.OpenError = nil
				_, err := rdsBroker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).ToNot(HaveOccurred())
				Expect(sqlEngine.OpenCalled).To(BeTrue())

				Expect(sqlEngine.OpenPassword).To(BeEquivalentTo(expectedMasterPassword))
			})
		})
	})

})
