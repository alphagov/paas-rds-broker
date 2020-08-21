package rdsbroker

import (
	"errors"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParameterGroupsSource", func() {

	Describe("composeGroupName", func() {
		var config Config
		var servicePlan ServicePlan
		var extensions []string
		var supportedPreloads map[string][]DBExtension

		BeforeEach(func() {
			config = Config{
				DBPrefix:   "foo",
				BrokerName: "envname",
			}

			servicePlan = ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("10"),
					EngineFamily:  aws.String("postgres10"),
				},
			}

			supportedPreloads = map[string][]DBExtension{
				"postgres10": {
					DBExtension{
						Name:                   "pg_stat_statements",
						RequiresPreloadLibrary: true,
					},
				},
			}
		})

		It("prepends the configured dbprefix", func() {
			name := composeGroupName(config, servicePlan, extensions, map[string][]DBExtension{})
			Expect(name).To(HavePrefix(config.DBPrefix))
		})

		It("contains the normalised engine family", func() {
			servicePlan.RDSProperties.EngineFamily = aws.String("test-db-engine-family")
			name := composeGroupName(config, servicePlan, extensions, map[string][]DBExtension{})
			Expect(name).To(ContainSubstring("testdbenginefamily"))
		})

		It("contains the broker name", func() {
			name := composeGroupName(config, servicePlan, extensions, map[string][]DBExtension{})
			Expect(name).To(ContainSubstring("envname"))
		})

		Context("contains the names of extensions", func() {
			It("only if the db engine is postgres", func() {
				extensions = []string{"pg_stat_statements"}
				servicePlan.RDSProperties.Engine = aws.String("database")
				name := composeGroupName(config, servicePlan, extensions, map[string][]DBExtension{})
				Expect(name).ToNot(HaveSuffix("pgstatstatements"))
			})

			It("which have been normalised", func() {
				extensions = []string{"pg_stat_statements"}
				name := composeGroupName(config, servicePlan, extensions, supportedPreloads)
				Expect(name).To(HaveSuffix("pgstatstatements"))
			})

			It("which require a pre-load library for that engine version", func() {
				extensions = []string{"pg_stat_statements", "notanext"}
				name := composeGroupName(config, servicePlan, extensions, supportedPreloads)
				Expect(name).To(HaveSuffix("pgstatstatements"))
				Expect(name).ToNot(ContainSubstring("notanext"))
			})

			It("dash-separates extension names", func() {
				extensions = []string{"pg_stat_statements", "pg_z"}

				supportedPreloads["postgres10"] = append(supportedPreloads["postgres10"], DBExtension{
					Name:                   "pg_z",
					RequiresPreloadLibrary: true,
				})

				name := composeGroupName(config, servicePlan, extensions, supportedPreloads)

				Expect(name).To(HaveSuffix("pgstatstatements-pgz"))
			})

			It("orders the extensions alphabetically", func() {
				extensions = []string{"pg_stat_statements", "pg_a", "pg_z"}

				supportedPreloads["postgres10"] = append(supportedPreloads["postgres10"], DBExtension{
					Name:                   "pg_a",
					RequiresPreloadLibrary: true,
				})

				supportedPreloads["postgres10"] = append(supportedPreloads["postgres10"], DBExtension{
					Name:                   "pg_z",
					RequiresPreloadLibrary: true,
				})

				name := composeGroupName(config, servicePlan, extensions, supportedPreloads)

				Expect(name).To(HaveSuffix("pga-pgstatstatements-pgz"))
			})
		})
	})

	Describe("SelectParameterGroup", func() {
		var config Config
		var servicePlan ServicePlan
		var extensions []string
		var rdsFake *fakes.FakeRDSInstance
		var supportedPreloads map[string][]DBExtension

		var parameterGroupSource *ParameterGroupSource

		BeforeEach(func() {
			config = Config{
				DBPrefix:   "rdsbroker",
				BrokerName: "envname",
			}

			servicePlan = ServicePlan{
				ID:   "test-1",
				Name: "Test",
				Free: aws.Bool(false),
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("10"),
					EngineFamily:  aws.String("postgres10"),
				},
			}

			logger := lager.NewLogger("rdsbroker_test")
			gingkoSink := lager.NewWriterSink(GinkgoWriter, lager.INFO)
			logger.RegisterSink(gingkoSink)
			testSink := lagertest.NewTestSink()
			logger.RegisterSink(testSink)

			supportedPreloads = map[string][]DBExtension{
				"postgres10": {
					DBExtension{
						Name:                   "pg_stat_statements",
						RequiresPreloadLibrary: true,
					},
				},
			}

			rdsFake = &fakes.FakeRDSInstance{}
			parameterGroupSource = NewParameterGroupSource(config, rdsFake, supportedPreloads, logger)
		})

		It("returns an error when the RDS api returns an error other than not found", func() {
			rdsError := awserr.New(rds.ErrCodeDBClusterAlreadyExistsFault, "not found", nil)
			rdsFake.GetParameterGroupReturns(nil, rdsError)

			_, err := parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
			Expect(err).To(HaveOccurred())
		})

		Describe("when the parameter group exists", func() {
			BeforeEach(func() {
				rdsFake.GetParameterGroupReturns(&rds.DBParameterGroup{
					DBParameterGroupArn:    aws.String("aws:arn:::db-parameter-group"),
					DBParameterGroupFamily: aws.String("postgres"),
					DBParameterGroupName:   aws.String("rdsbroker-postgres10-envname"),
					Description:            aws.String("rdsbroker-postgres10-envname"),
				}, nil)
			})

			It("does not attempt to create the group", func() {
				parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
				Expect(rdsFake.CreateParameterGroupCallCount()).To(Equal(0))
			})

			It("returns the group name", func() {
				name, _ := parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
				Expect(name).To(Equal("rdsbroker-postgres10-envname"))
			})
		})

		Describe("when the parameter group does not exist", func() {
			BeforeEach(func() {
				rdsFake.GetParameterGroupReturns(nil, errors.New(rds.ErrCodeDBParameterGroupNotFoundFault+": errMsg"))
			})

			It("attempts to create the group", func() {
				rdsFake.CreateParameterGroupReturns(nil)

				parameterGroupSource.SelectParameterGroup(servicePlan, extensions)

				Expect(rdsFake.CreateParameterGroupCallCount()).To(Equal(1))
				createDBParameterGroupInput := rdsFake.CreateParameterGroupArgsForCall(0)
				Expect(aws.StringValue(createDBParameterGroupInput.DBParameterGroupName)).To(Equal("rdsbroker-postgres10-envname"))
			})

			It("sets the group family from the configured plan", func() {
				rdsFake.CreateParameterGroupReturns(nil)
				servicePlan.RDSProperties.EngineFamily = aws.String("postgres10-cfg")

				parameterGroupSource.SelectParameterGroup(servicePlan, extensions)

				Expect(rdsFake.CreateParameterGroupCallCount()).To(Equal(1))
				createDBParameterGroupInput := rdsFake.CreateParameterGroupArgsForCall(0)
				Expect(aws.StringValue(createDBParameterGroupInput.DBParameterGroupFamily)).To(Equal(aws.StringValue(servicePlan.RDSProperties.EngineFamily)))
			})

			It("returns an error if creating the parameter group fails", func() {
				createError := awserr.New(rds.ErrCodeDBParameterGroupAlreadyExistsFault, "exists", nil)
				rdsFake.CreateParameterGroupReturns(createError)

				_, err := parameterGroupSource.SelectParameterGroup(servicePlan, extensions)

				Expect(err).To(HaveOccurred())
			})

			Describe("it modifies the created parameter group", func() {
				It("does not make any changes to the parameter group for MySQL databases", func() {
					servicePlan.RDSProperties.Engine = aws.String("mysql")
					servicePlan.RDSProperties.EngineVersion = aws.String("5.7")
					servicePlan.RDSProperties.EngineFamily = aws.String("mysql5.7")

					rdsFake.ModifyParameterGroupReturns(nil)

					parameterGroupSource.SelectParameterGroup(servicePlan, extensions)

					Expect(rdsFake.ModifyParameterGroupCallCount()).To(Equal(0), "ModifyParameterGroup was called when it shouldn't have been")
				})

				It("and sets the force SSL property", func() {
					rdsFake.ModifyParameterGroupReturns(nil)

					parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
					Expect(rdsFake.ModifyParameterGroupCallCount()).To(Equal(1), "ModifyParameterGroup was not called")

					modifyInput := rdsFake.ModifyParameterGroupArgsForCall(0)

					var relevantParam *rds.Parameter = nil
					for _, param := range modifyInput.Parameters {
						if aws.StringValue(param.ParameterName) == "rds.force_ssl" {
							relevantParam = param
						}
					}

					Expect(relevantParam).ToNot(BeNil())
					Expect(aws.StringValue(relevantParam.ParameterValue)).To(Equal("1"))
					Expect(aws.StringValue(relevantParam.ApplyMethod)).To(Equal("pending-reboot"))
				})

				It("and sets the log retention period", func() {
					rdsFake.ModifyParameterGroupReturns(nil)

					parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
					Expect(rdsFake.ModifyParameterGroupCallCount()).To(Equal(1), "ModifyParameterGroup was not called")

					modifyInput := rdsFake.ModifyParameterGroupArgsForCall(0)

					var relevantParam *rds.Parameter = nil
					for _, param := range modifyInput.Parameters {
						if aws.StringValue(param.ParameterName) == "rds.log_retention_period" {
							relevantParam = param
						}
					}

					Expect(relevantParam).ToNot(BeNil())
					Expect(aws.StringValue(relevantParam.ParameterValue)).To(Equal("10080"))
					Expect(aws.StringValue(relevantParam.ApplyMethod)).To(Equal("immediate"))
				})

				It("and enabled logical replication", func() {
					rdsFake.ModifyParameterGroupReturns(nil)

					parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
					Expect(rdsFake.ModifyParameterGroupCallCount()).To(Equal(1), "ModifyParameterGroup was not called")

					modifyInput := rdsFake.ModifyParameterGroupArgsForCall(0)

					var relevantParam *rds.Parameter = nil
					for _, param := range modifyInput.Parameters {
						if aws.StringValue(param.ParameterName) == "rds.logical_replication" {
							relevantParam = param
						}
					}

					Expect(relevantParam).ToNot(BeNil())
					Expect(aws.StringValue(relevantParam.ParameterValue)).To(Equal("1"))
					Expect(aws.StringValue(relevantParam.ApplyMethod)).To(Equal("immediate"))
				})
			})

			It("when an extension requires a preload library, it modifies the parameter group to add it", func() {
				extensions = []string{"pg_stat_statements", "pg_super_ext"}

				supportedPreloads["postgres10"] = append(supportedPreloads["postgres10"], DBExtension{
					Name:                   "pg_super_ext",
					RequiresPreloadLibrary: true,
				})

				rdsFake.ModifyParameterGroupReturns(nil)

				parameterGroupSource.SelectParameterGroup(servicePlan, extensions)

				Expect(rdsFake.ModifyParameterGroupCallCount()).To(Equal(1), "ModifyParameterGroup was not called")

				modifyInput := rdsFake.ModifyParameterGroupArgsForCall(0)

				var relevantParam *rds.Parameter = nil
				for _, param := range modifyInput.Parameters {
					if aws.StringValue(param.ParameterName) == "shared_preload_libraries" {
						relevantParam = param
					}
				}

				Expect(relevantParam).ToNot(BeNil())
				Expect(aws.StringValue(relevantParam.ParameterValue)).To(Equal("pg_stat_statements,pg_super_ext"))
				Expect(aws.StringValue(relevantParam.ApplyMethod)).To(Equal("pending-reboot"))
			})

			It("when no preload libraries are needed, it does not set the shared_preload_libraries parameter, because it's value cannot be empty", func() {
				extensions = []string{"postgis"}
				servicePlan.RDSProperties.AllowedExtensions = []*string{aws.String("postgis")}

				rdsFake.ModifyParameterGroupReturns(nil)

				parameterGroupSource.SelectParameterGroup(servicePlan, extensions)
				Expect(rdsFake.ModifyParameterGroupCallCount()).To(Equal(1), "ModifyParameterGroup was not called")

				modifyInput := rdsFake.ModifyParameterGroupArgsForCall(0)
				discovered := false
				for _, param := range modifyInput.Parameters {
					if aws.StringValue(param.ParameterName) == "shared_preload_libraries" {
						discovered = true
						break
					}
				}

				Expect(discovered).To(BeFalse(), "The shared_preload_libraries property was set when it shouldn't have been")
			})

		})

	})
})
