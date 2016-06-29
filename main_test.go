package main

import (
	"net/http"
	"net/http/httptest"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/pivotal-golang/lager"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Main", func() {

	Describe("constructing the top-level HTTP handler", func() {

		It("has a healthcheck endpoint that returns 200", func() {
			handler := buildHTTPHandler(
				&rdsbroker.RDSBroker{},
				lager.NewLogger("main.test"),
				&Config{},
			)
			req, err := http.NewRequest("GET", "http://example.com/healthcheck", nil)
			Expect(err).NotTo(HaveOccurred())

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			Expect(w.Code).To(Equal(200))
		})
	})
})
