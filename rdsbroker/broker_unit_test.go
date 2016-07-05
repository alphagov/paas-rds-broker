package rdsbroker

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RDS Broker internals", func() {
	var (
		broker *RDSBroker
	)

	BeforeEach(func() {
		broker = &RDSBroker{
			dbPrefix: "cf",
		}
	})

	Describe("dbInstanceIdentifier", func() {
		It("combines the dbPrefix with the instanceID", func() {
			Expect(broker.dbInstanceIdentifier("a8051869-696b-4031-a290-8f45588f308c")).To(Equal("cf-a8051869-696b-4031-a290-8f45588f308c"))
		})

		It("replaces '_'s in the instanceID with '-'s", func() {
			Expect(broker.dbInstanceIdentifier("with-dash_underscore")).To(Equal("cf-with-dash-underscore"))
		})

		It("replaces '_'s in the dbPrefix with '-'s", func() {
			broker.dbPrefix = "with-dash_underscore"
			Expect(broker.dbInstanceIdentifier("123")).To(Equal("with-dash-underscore-123"))
		})
	})

	Describe("dbInstanceIdentifierToServiceInstanceID", func() {

		It("strips the dbPrefix off", func() {
			actual := broker.dbInstanceIdentifierToServiceInstanceID("cf-a8051869-696b-4031-a290-8f45588f308c")
			Expect(actual).To(Equal("a8051869-696b-4031-a290-8f45588f308c"))
		})

		It("handles '_'s in the dbPrefix", func() {
			broker.dbPrefix = "with-dash_underscore"
			actual := broker.dbInstanceIdentifierToServiceInstanceID("with-dash-underscore-123")
			Expect(actual).To(Equal("123"))
		})
	})
})
