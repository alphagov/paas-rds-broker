package rdsbroker_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestParameterGroupsSource(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Parameter Groups Source Suite")
}
