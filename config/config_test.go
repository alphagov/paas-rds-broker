package config_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/config"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

var _ = Describe("Config", func() {
	var (
		config Config

		validConfig = Config{
			LogLevel: "DEBUG",
			Username: "broker-username",
			Password: "broker-password",
			RDSConfig: &rdsbroker.Config{
				Region:             "rds-region",
				DBPrefix:           "cf",
				BrokerName:         "mybroker",
				AWSPartition:       "rds-partition",
				MasterPasswordSeed: "secret",
			},
			CronSchedule:         "@hourly",
			KeepSnapshotsForDays: 1,
		}
	)

	Describe("Validate", func() {
		BeforeEach(func() {
			config = validConfig
		})

		It("does not return error if all sections are valid", func() {
			err := config.Validate()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if LogLevel is not valid", func() {
			config.LogLevel = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty LogLevel"))
		})

		It("returns error if Username is not valid", func() {
			config.Username = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Username"))
		})

		It("returns error if Password is not valid", func() {
			config.Password = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Must provide a non-empty Password"))
		})

		It("returns error if RDS configuration is not valid", func() {
			config.RDSConfig = &rdsbroker.Config{}

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Validating RDS configuration"))
		})

		It("returns an error if cron schedule is empty", func() {
			config.CronSchedule = ""

			err := config.Validate()
			Expect(err).To(MatchError("must provide a non-empty cron_schedule"))
		})

		It("returns an error if keep_snapshots_for_days is missing", func() {
			config.KeepSnapshotsForDays = 0

			err := config.Validate()
			Expect(err).To(MatchError("must provide a valid number for keep_snapshots_for_days"))
		})
	})
})
