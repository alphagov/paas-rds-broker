package sqlengine_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSQLEngine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQL Engine Suite")
}
