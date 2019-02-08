package rdsbroker

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"strings"
)

var _ = Describe("ParameterGroupsSource", func() {

	Describe("decodeName", func() {

		var servicePlan ServicePlan

		BeforeEach(func() {
			servicePlan = ServicePlan{
				ID:          "service-1",
				Name:        "svc-1",
				Description: "",
				Free:        aws.Bool(false),
				Metadata:    nil,
				RDSProperties: RDSProperties{
					DBInstanceClass:            nil,
					Engine:                     aws.String("postgres"),
					EngineVersion:              aws.String("9.5"),
					AllocatedStorage:           nil,
					AutoMinorVersionUpgrade:    nil,
					AvailabilityZone:           nil,
					BackupRetentionPeriod:      nil,
					CharacterSetName:           nil,
					DBParameterGroupName:       nil,
					DBSecurityGroups:           nil,
					DBSubnetGroupName:          nil,
					LicenseModel:               nil,
					MultiAZ:                    nil,
					OptionGroupName:            nil,
					Port:                       nil,
					PreferredBackupWindow:      nil,
					PreferredMaintenanceWindow: nil,
					PubliclyAccessible:         nil,
					StorageEncrypted:           nil,
					KmsKeyID:                   nil,
					StorageType:                nil,
					Iops:                       nil,
					VpcSecurityGroupIds:        nil,
					CopyTagsToSnapshot:         nil,
					SkipFinalSnapshot:          nil,
					DefaultExtensions:          nil,
				},
			}
		})

		DescribeTable("decoding names takes in to account the configurable database prefix",
			func(prefix string, groupName string) {
				selected, err := decodeName(groupName, servicePlan, prefix)

				Expect(err).ToNot(HaveOccurred())
				Expect(selected.Name).To(Equal(groupName))
				Expect(selected.Name).To(ContainSubstring(prefix))
			},
			Entry("standard rdsbroker prefix", "rdsbroker", "rdsbroker-postgres95-envname"),
			Entry("integration test prefix", "build-test", "build-test-postgres95-envname"),
		)

		It("returns an error when a name contains too few fields", func() {
			groupName := "malformed-name"
			_, err := decodeName(groupName, servicePlan, "rdsbroker")

			Expect(err).Should(HaveOccurred())
		})

		It("can decode a group with name zero extensions", func() {
			groupName := "rdsbroker-postgres95-envname"
			pg, _ := decodeName(groupName, servicePlan, "rdsbroker")

			Expect(pg.Engine).To(Equal("postgres"))
			Expect(pg.EngineVersion).To(Equal("95"))
			Expect(pg.Extensions).To(BeEmpty())
		})

		It("feeds back the name of the parameter group in the name property", func() {
			groupName := "rdsbroker-postgres95-envname"
			pg, _ := decodeName(groupName, servicePlan, "rdsbroker")

			Expect(pg.Name).To(Equal(groupName))
		})

		DescribeTable("can decode the name and version of the engine",
			func(engineString string, expectedEngine string, expectedVersion string) {
				groupName := fmt.Sprintf("rdsbroker-%s-envname", engineString)
				pg, _ := decodeName(groupName, servicePlan, "rdsbroker")

				Expect(pg.EngineVersion).To(Equal(expectedVersion))
				Expect(pg.Engine).To(Equal(expectedEngine))
			},
			Entry("using postgres10", "postgres10", "postgres", "10"),
			Entry("using postgres95", "postgres95", "postgres", "95"),
			Entry("using mysql57", "mysql57", "mysql", "57"),
		)

		Describe("matching extensions", func() {
			var postgres95Entries = []TableEntry{}
			for _, ex := range SupportedPreloadExtensions["postgres9.5"] {
				denormalisedExtName := strings.Replace(ex.Name, "_", "-", -1)
				postgres95Entries = append(postgres95Entries, Entry(ex.Name, denormalisedExtName, ex.Name))
			}
			DescribeTable("matching supported extensions which require preload libraries for postgres 10",
				func(inputExtName string, expectedExtName string) {
					groupName := fmt.Sprintf("rdsbroker-postgres95-envname-%s", inputExtName)
					pg, _ := decodeName(groupName, servicePlan, "rdsbroker")

					Expect(pg.EngineVersion).To(Equal("95"))
					Expect(pg.Engine).To(Equal("postgres"))
					Expect(pg.Extensions).To(ContainElement(expectedExtName))
				},
				postgres95Entries...,
			)
		})
	})

	Describe("SelectParameterGroup", func() {
		var config Config
		var servicePlan ServicePlan
		var provisionDetails ProvisionParameters
		var postgres10ExtensionsBackup []DBExtension

		BeforeEach(func() {
			config = Config{
				Region:                       "",
				DBPrefix:                     "rdsbroker",
				BrokerName:                   "",
				AWSPartition:                 "",
				MasterPasswordSeed:           "",
				AllowUserProvisionParameters: false,
				AllowUserUpdateParameters:    false,
				AllowUserBindParameters:      false,
				Catalog:                      Catalog{},
				ParameterGroups:              nil,
			}

			servicePlan = ServicePlan{
				ID:   "test-1",
				Name: "Test",
				Free: aws.Bool(false),
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("10"),
				},
			}

			provisionDetails = ProvisionParameters{}

			postgres10ExtensionsBackup = SupportedPreloadExtensions["postgres10"]
		})

		AfterEach(func() {
			SupportedPreloadExtensions["postgres10"] = postgres10ExtensionsBackup
		})

		Describe("selecting based on engine version", func() {
			It("can select mysql5.7", func() {
				expected := "rdsbroker-mysql57-envname"
				config.ParameterGroups = []string{
					"rdsbroker-postgres95-envname",
					expected,
				}

				servicePlan.RDSProperties.Engine = aws.String("mysql")
				servicePlan.RDSProperties.EngineVersion = aws.String("5.7")

				parameterGroupSource := NewParameterGroupSource(config)

				selectedParamGroup, _ := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(selectedParamGroup.Name).To(Equal(expected))
				Expect(selectedParamGroup.Engine).To(Equal("mysql"))
				Expect(selectedParamGroup.EngineVersion).To(Equal("57"))
			})

			It("can select postgres10", func() {
				expected := "rdsbroker-postgres10-envname"
				config.ParameterGroups = []string{
					"rdsbroker-postgres95-envname",
					expected,
					"rdsbroker-mysql57-envname",
				}

				parameterGroupSource := NewParameterGroupSource(config)

				selectedParamGroup, _ := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(selectedParamGroup.Name).To(Equal(expected))
				Expect(selectedParamGroup.Engine).To(Equal("postgres"))
				Expect(selectedParamGroup.EngineVersion).To(Equal("10"))
			})

			It("can select postgres9.5", func() {
				expected := "rdsbroker-postgres95-envname"
				config.ParameterGroups = []string{
					"rdsbroker-mysql57-envname",
					"rdsbroker-postgres10-envname",
					expected,
				}

				servicePlan.RDSProperties.Engine = aws.String("postgres")
				servicePlan.RDSProperties.EngineVersion = aws.String("9.5")

				parameterGroupSource := NewParameterGroupSource(config)

				selectedParamGroup, _ := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(selectedParamGroup.Name).To(Equal(expected))
				Expect(selectedParamGroup.Engine).To(Equal("postgres"))
				Expect(selectedParamGroup.EngineVersion).To(Equal("95"))
			})
		})

		Describe("selecting based on engine version and extensions", func() {
			It("will choose a parameter group with no extensions enabled if no extensions are requested", func() {
				expected := "rdsbroker-postgres10-envname"
				config.ParameterGroups = []string{
					"rdsbroker-postgres10-envname-pg-stat-statements",
					expected,
				}
				provisionDetails.Extensions = []string{}

				parameterGroupSource := NewParameterGroupSource(config)
				selectedParamGroup, _ := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(selectedParamGroup.Name).To(Equal(expected))
				Expect(selectedParamGroup.Extensions).To(BeEmpty())
			})

			It("will choose a parameter group with the most matching extensions, if any extensions are requested", func() {
				expected := "rdsbroker-postgres10-envname-pg-stat-statements"
				config.ParameterGroups = []string{
					expected,
					"rdsbroker-postgres10-envname",
				}
				provisionDetails.Extensions = []string{"pg_stat_statements"}

				parameterGroupSource := NewParameterGroupSource(config)
				selectedParamGroup, _ := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(selectedParamGroup.Name).To(Equal(expected))
				Expect(selectedParamGroup.Extensions).To(ContainElement("pg_stat_statements"))
			})

			It("will return an error if no extensions are required and there are no applicable groups with no extensions enabled", func() {
				config.ParameterGroups = []string{
					"rdsbroker-postgres10-envname-pg-stat-statements",
				}

				provisionDetails.Extensions = []string{}

				parameterGroupSource := NewParameterGroupSource(config)
				_, err := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(err).Should(HaveOccurred())
			})

			It("does not attempt to satisfy extensions which don't need preload libraries", func() {
				expected := "rdsbroker-postgres95-envname-pg-stat-statements"
				config.ParameterGroups = []string{
					"rdsbroker-postgres95-envname",
					expected,
				}
				provisionDetails.Extensions = []string{"pg_stat_statements", "foo"}
				servicePlan.RDSProperties.EngineVersion = aws.String("9.5")

				parameterGroupSource := NewParameterGroupSource(config)
				selectedParamGroup, err := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(selectedParamGroup.Name).To(Equal(expected))
			})

			It("will return an error if it cannot satisfy all relevant extensions", func() {
				config.ParameterGroups = []string{
					"rdsbroker-postgres10-envname-pg-stat-statements",
					"rdsbroker-postgres10-envname",
				}
				provisionDetails.Extensions = []string{"pg_stat_statements", "foo"}

				SupportedPreloadExtensions["postgres10"] = append(SupportedPreloadExtensions["postgres10"], DBExtension{
					Name:                   "foo",
					RequiresPreloadLibrary: true,
				})

				parameterGroupSource := NewParameterGroupSource(config)
				_, err := parameterGroupSource.SelectParameterGroup(servicePlan, provisionDetails)

				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
