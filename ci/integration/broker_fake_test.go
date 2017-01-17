package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/frodenas/brokerapi"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
	"github.com/alphagov/paas-rds-broker/config"

	rdsfake "github.com/alphagov/paas-rds-broker/awsrds/fakes"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
	sqlfake "github.com/alphagov/paas-rds-broker/sqlengine/fakes"
)

var _ = Describe("RDS Broker", func() {

	var (
		allowUserProvisionParameters bool
		allowUserUpdateParameters    bool
		allowUserBindParameters      bool
		serviceBindable              bool
		planUpdateable               bool
		skipFinalSnapshot            bool
		dbPrefix                     string
		brokerName                   string
		dbInstance                   *rdsfake.FakeDBInstance
		rdsBrokerConfig              *config.Config

		sqlProvider *sqlfake.FakeProvider
		sqlEngine   *sqlfake.FakeSQLEngine

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsBroker *rdsbroker.RDSBroker

		rdsBrokerServer http.Handler
	)
	const ()

	BeforeEach(func() {
		var err error

		allowUserProvisionParameters = true
		allowUserUpdateParameters = true
		allowUserBindParameters = true
		serviceBindable = true
		planUpdateable = true
		skipFinalSnapshot = true
		dbPrefix = "cf"
		brokerName = "mybroker"

		dbInstance = &rdsfake.FakeDBInstance{}
		sqlProvider = &sqlfake.FakeProvider{}
		sqlEngine = &sqlfake.FakeSQLEngine{}
		sqlProvider.GetSQLEngineSQLEngine = sqlEngine

		rdsBrokerConfig, err = config.LoadConfig("./config.json")
		Expect(err).ToNot(HaveOccurred())

	})

	JustBeforeEach(func() {
		var err error

		Expect(err).ToNot(HaveOccurred())

		logger = lager.NewLogger("rdsbroker_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsBroker = rdsbroker.New(*rdsBrokerConfig.RDSConfig, dbInstance, sqlProvider, logger)

		credentials := brokerapi.BrokerCredentials{
			Username: rdsBrokerConfig.Username,
			Password: rdsBrokerConfig.Password,
		}

		rdsBrokerServer = brokerapi.New(rdsBroker, logger, credentials)

	})

	var _ = Describe("Services", func() {
		It("returns the proper CatalogResponse", func() {
			var err error

			recorder := httptest.NewRecorder()

			req, _ := http.NewRequest("GET", "http://example.com/v2/catalog", nil)
			req.SetBasicAuth(rdsBrokerConfig.Username, rdsBrokerConfig.Password)

			rdsBrokerServer.ServeHTTP(recorder, req)
			Expect(recorder.Code).To(Equal(200))

			catalog := brokerapi.CatalogResponse{}
			err = json.Unmarshal(recorder.Body.Bytes(), &catalog)
			Expect(err).ToNot(HaveOccurred())

			sort.Sort(ByServiceID(catalog.Services))

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
			Expect(service1.PlanUpdateable).To(BeTrue())
			Expect(service1.Plans).To(HaveLen(1))
			Expect(service1.Plans[0].ID).To(Equal("Plan-1"))
			Expect(service1.Plans[0].Name).To(Equal("Plan 1"))
			Expect(service1.Plans[0].Description).To(Equal("This is the Plan 1"))
		})

	})

	var _ = Describe("Provision", func() {
		var (
			provisionDetailsJson []byte
			serviceID            string
			acceptsIncomplete    bool
		)

		BeforeEach(func() {
			provisionDetailsJson = []byte(`
				{
					"service_id": "Service-1",
					"plan_id": "Plan-1",
					"organization_guid": "organization-id",
					"space_guid": "space-id",
					"parameters": {}
				}
			`)
			serviceID = "Service-1"
			acceptsIncomplete = true
		})

		var doProvisionRequest = func() *httptest.ResponseRecorder {
			recorder := httptest.NewRecorder()

			path := "/v2/service_instances/" + serviceID

			if acceptsIncomplete {
				path = path + "?accepts_incomplete=true"
			}

			req, _ := http.NewRequest("PUT", path, bytes.NewBuffer(provisionDetailsJson))
			req.SetBasicAuth(rdsBrokerConfig.Username, rdsBrokerConfig.Password)

			rdsBrokerServer.ServeHTTP(recorder, req)

			return recorder
		}

		It("returns 202 Accepted, Service instance provisioning is in progress", func() {
			recorder := doProvisionRequest()
			Expect(recorder.Code).To(Equal(202))
		})
	})

})
