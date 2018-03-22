package cron_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/cron"
)

var _ = Describe("Cron Config", func() {
	var (
		config Config
	)

	Describe("Validate", func() {
		BeforeEach(func() {
			config = Config{
				RDSConfig: RDSConfig{
					Region:     "eu-west-1",
					BrokerName: "test-broker",
				},
				LogLevel:             "DEBUG",
				CronSchedule:         "@hourly",
				KeepSnapshotsForDays: 1,
			}
		})

		It("does not return error if all sections are valid", func() {
			err := config.Validate()
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns error if LogLevel is not valid", func() {
			config.LogLevel = ""

			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must provide a non-empty log_level"))
		})

		It("returns an error if aws region is empty", func() {
			config.RDSConfig.Region = ""

			err := config.Validate()
			Expect(err).To(MatchError("must provide a non-empty rds_config.region"))
		})

		It("returns an error if broker name is empty", func() {
			config.RDSConfig.BrokerName = ""

			err := config.Validate()
			Expect(err).To(MatchError("must provide a non-empty rds_config.broker_name"))
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
