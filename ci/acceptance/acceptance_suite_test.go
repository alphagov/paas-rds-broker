package acceptance_test

import (
	"crypto/tls"
	"net/http"
	"testing"

	cfg "github.com/cloudfoundry-incubator/cf-test-helpers/config"
	"github.com/cloudfoundry-incubator/cf-test-helpers/helpers"
	"github.com/cloudfoundry-incubator/cf-test-helpers/workflowhelpers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	config     Config
	cfConfig   *cfg.Config
	httpClient *http.Client
)

func TestAcceptance(t *testing.T) {
	RegisterFailHandler(Fail)

	cfConfig = cfg.LoadConfig()
	httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfConfig.SkipSSLValidation},
		},
	}

	testSetup := workflowhelpers.NewTestSuiteSetup(cfConfig)

	BeforeSuite(func() {
		testSetup.Setup()
	})

	AfterSuite(func() {
		testSetup.Teardown()
	})

	reporters := []Reporter{}
	if cfConfig.GetArtifactsDirectory() != "" {
		helpers.EnableCFTrace(cfConfig, "CATS-RDS")
		reporters = append(reporters, helpers.NewJUnitReporter(cfConfig, "CATS-RDS"))
	}

	RunSpecsWithDefaultAndCustomReporters(t, "Acceptance Suite", reporters)
}
