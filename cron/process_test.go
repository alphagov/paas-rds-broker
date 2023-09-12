package cron_test

import (
	"errors"

	"code.cloudfoundry.org/lager"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/alphagov/paas-rds-broker/config"
	. "github.com/alphagov/paas-rds-broker/cron"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

var _ = Describe("Process", func() {

	var cfg *config.Config
	var rdsInstance *fakes.FakeRDSInstance
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
		rdsInstance = &fakes.FakeRDSInstance{}
		logger = lager.NewLogger("main.test")
		process = NewProcess(cfg, rdsInstance, logger)
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
			return rdsInstance.DeleteSnapshotsCallCount()
		}, "5s").Should(BeNumerically(">=", 2))

		brokerName, keepForDays := rdsInstance.DeleteSnapshotsArgsForCall(0)
		Expect(brokerName).To(Equal("test-broker"))
		Expect(keepForDays).To(Equal(7))

		brokerName, keepForDays = rdsInstance.DeleteSnapshotsArgsForCall(1)
		Expect(brokerName).To(Equal("test-broker"))
		Expect(keepForDays).To(Equal(7))

		Expect(err).ToNot(HaveOccurred())
	})

	It("should continue on error", func() {
		var err error
		go func() {
			err = process.Start()
		}()

		rdsInstance.DeleteSnapshotsReturns(errors.New("some error"))
		Eventually(func() int {
			return rdsInstance.DeleteSnapshotsCallCount()
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
