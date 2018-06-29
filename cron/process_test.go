package cron_test

import (
	"errors"

	"code.cloudfoundry.org/lager"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/alphagov/paas-rds-broker/config"
	. "github.com/alphagov/paas-rds-broker/cron"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

var _ = Describe("Process", func() {

	var cfg *config.Config
	var dbInstance *fakes.FakeDBInstance
	var logger lager.Logger
	var process *Process

	BeforeEach(func() {
		cfg = &config.Config{
			RDSConfig: &rdsbroker.Config{
				Region:     "eu-west-1",
				BrokerName: "test-broker",
			},
			KeepSnapshotsForDays: 7,
			CronSchedule:         "* * * * *",
		}
		dbInstance = &fakes.FakeDBInstance{}
		logger = lager.NewLogger("main.test")
		process = NewProcess(cfg, dbInstance, logger)
	})

	AfterEach(func() {
		process.Stop()
	})

	It("should delete the old snapshots regularly", func() {
		var err error
		go func() {
			err = process.Start()
		}()

		Eventually(func() int {
			return dbInstance.DeleteSnapshotsCallCount
		}, "5s").Should(BeNumerically(">=", 2))

		Expect(dbInstance.DeleteSnapshotsBrokerName[0]).To(Equal("test-broker"))
		Expect(dbInstance.DeleteSnapshotsKeepForDays[0]).To(Equal(7))
		Expect(dbInstance.DeleteSnapshotsBrokerName[1]).To(Equal("test-broker"))
		Expect(dbInstance.DeleteSnapshotsKeepForDays[1]).To(Equal(7))

		Expect(err).ToNot(HaveOccurred())
	})

	It("should continue on error", func() {
		var err error
		go func() {
			err = process.Start()
		}()

		dbInstance.DeleteSnapshotsError = []error{errors.New("some error")}
		Eventually(func() int {
			return dbInstance.DeleteSnapshotsCallCount
		}, "5s").Should(BeNumerically(">=", 2))

		Expect(err).ToNot(HaveOccurred())
	})

	Context("the schedule is invalid", func() {
		It("should exit with error", func() {
			cfg.CronSchedule = "invalid"
			err := process.Start()
			Expect(err).To(MatchError("cron_schedule is invalid: Expected 5 to 6 fields, found 1: invalid"))
		}, 2)
	})

})
