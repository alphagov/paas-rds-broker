package rdsbroker_test

import (
	"github.com/aws/aws-sdk-go/aws"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/rdsbroker"
)

var _ = Describe("Config", func() {
	var (
		config Config

		validConfig = Config{
			Region:             "rds-region",
			DBPrefix:           "cf",
			MasterPasswordSeed: "secret",
			BrokerName:         "mybroker",
			AWSPartition:       "rds-partition",
			Catalog: Catalog{
				Services: []Service{
					Service{
						ID:          "service-1",
						Name:        "Service 1",
						Description: "Service 1 description",
					},
				},
				ExcludeEngines: []Engine{},
			},
		}
	)

	Describe("FillDefaults", func() {
		BeforeEach(func() {
			config = validConfig
		})

		It("sets default aws partition if empty", func() {
			config.AWSPartition = ""
			config.FillDefaults()
			Expect(config.AWSPartition).To(Equal("aws"))
		})

		It("preserves aws partition if not empty", func() {
			config.FillDefaults()
			Expect(config.AWSPartition).To(Equal("rds-partition"))
		})
	})

	Describe("Validate", func() {
		BeforeEach(func() {
			config = validConfig
		})

		It("does not return error if all sections are valid", func() {
			err := config.Validate()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if Region is not valid", func() {
			config.Region = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Region"))
		})

		It("returns error if DBPrefix is not valid", func() {
			config.DBPrefix = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty DBPrefix"))
		})

		It("returns error if MasterPasswordSeed is not valid", func() {
			config.MasterPasswordSeed = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty MasterPasswordSeed"))
		})

		It("returns error if BrokerName is not valid", func() {
			config.BrokerName = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty BrokerName"))
		})

		It("returns error if Catalog is not valid", func() {
			config.Catalog = Catalog{
				Services: []Service{
					Service{},
				},
				ExcludeEngines: []Engine{},
			}

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Validating Catalog configuration"))
		})
	})
})

var _ = Describe("ServicePlan", func() {
	Describe("IsUpgradeFrom", func() {
		It("returns an error if the engines are different", func() {
			planA := ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("10"),
				},
			}

			planB := ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("mysql"),
					EngineVersion: aws.String("8"),
				},
			}

			_, err := planB.IsUpgradeFrom(planA)
			Expect(err).To(HaveOccurred())
		})

		It("returns true if the input plans engine version is smaller", func() {
			planA := ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("12"),
				},
			}

			planB := ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("11"),
				},
			}

			actual, err := planA.IsUpgradeFrom(planB)
			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(BeTrue())
		})

		It("returns false if the input plans engine version is smaller", func() {
			planA := ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("10"),
				},
			}

			planB := ServicePlan{
				RDSProperties: RDSProperties{
					Engine:        aws.String("postgres"),
					EngineVersion: aws.String("11"),
				},
			}

			actual, err := planA.IsUpgradeFrom(planB)
			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(BeFalse())
		})
	})
})
