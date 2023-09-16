package awsrds_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAWSRDS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AWS RDS Suite")
}
