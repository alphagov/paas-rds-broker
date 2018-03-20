package main

import (
	"errors"
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/lager"
	"github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/alphagov/paas-rds-broker/config"
	"github.com/alphagov/paas-rds-broker/rdsbroker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Main", func() {

	Describe("constructing the top-level HTTP handler", func() {

		It("has a healthcheck endpoint that returns 200", func() {
			handler := buildHTTPHandler(
				&rdsbroker.RDSBroker{},
				lager.NewLogger("main.test"),
				&config.Config{},
			)
			req, err := http.NewRequest("GET", "http://example.com/healthcheck", nil)
			Expect(err).NotTo(HaveOccurred())

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			Expect(w.Code).To(Equal(200))
		})
	})

	Describe("the cron process", func() {
		var cfg *config.Config
		var dbInstance *fakes.FakeDBInstance
		var logger lager.Logger

		BeforeEach(func() {
			cfg = &config.Config{
				KeepSnapshotsForDays: 7,
				CronSchedule:         "* * * * *",
				RDSConfig: &rdsbroker.Config{
					BrokerName: "test-broker",
				},
			}
			dbInstance = &fakes.FakeDBInstance{}
			logger = lager.NewLogger("main.test")
		})

		AfterEach(func() {
			stopCron()
		})

		It("should delete the old snapshots regularly", func() {
			var err error
			go func() {
				err = startCron(cfg, dbInstance, logger)
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
				err = startCron(cfg, dbInstance, logger)
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
				err := startCron(cfg, dbInstance, logger)
				Expect(err).To(MatchError("cron_schedule is invalid: Expected 5 to 6 fields, found 1: invalid"))
			}, 2)
		})
	})

})
