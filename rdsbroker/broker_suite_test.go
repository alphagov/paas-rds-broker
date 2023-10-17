package rdsbroker_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRDSBroker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RDS Broker Suite")
}
