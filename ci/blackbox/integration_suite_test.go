package integration_aws_test

import (
	"bytes"
	"code.cloudfoundry.org/lager"
	"encoding/gob"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/alphagov/paas-rds-broker/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
)

type SuiteData struct {
	RdsBrokerPath   string
	RdsBrokerConfig *config.Config
}

var (
	suiteData SuiteData

	testSuiteLogger    lager.Logger
	rdsSubnetGroupName *string
	ec2SecurityGroupID *string
)

func TestSuite(t *testing.T) {
	SynchronizedBeforeSuite(
		func() []byte {
			testSuiteLogger = lager.NewLogger("test-suite-synchronization")
			testSuiteLogger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.INFO))

			logSess := testSuiteLogger.Session("before-suite")
			var err error

			// Compile the broker
			logSess.Info("compile-broker")
			rdsBrokerPath, err := gexec.Build("github.com/alphagov/paas-rds-broker")
			if err != nil {
				logSess.Error("compile-broker", err)
			}
			Expect(err).ShouldNot(HaveOccurred())

			// Update config
			logSess.Info("update-broker-config")
			rdsBrokerConfig, err := config.LoadConfig("./config.json")
			if err != nil {
				logSess.Error("update-broker-config", err)
			}
			Expect(err).ToNot(HaveOccurred())
			err = rdsBrokerConfig.Validate()
			if err != nil {
				logSess.Error("broker-config-invalid", err)
			}
			Expect(err).ToNot(HaveOccurred())

			// DB instance identifiers can be a maximum
			// of 63 characters. This leaves a budget of 27 characers
			// for the prefix.
			rdsBrokerConfig.RDSConfig.DBPrefix = fmt.Sprintf("%s-%d",
				"build-test",      // 10 characters
				time.Now().Unix(), // 10 characters
			)
			logSess.Info("db-prefix", lager.Data{"prefix": rdsBrokerConfig.RDSConfig.DBPrefix})

			awsSession, _ := session.NewSession(&aws.Config{
				Region: aws.String(rdsBrokerConfig.RDSConfig.Region)},
			)

			logSess.Info("create-subnet-group")
			rdsSubnetGroupName, err = CreateSubnetGroup(rdsBrokerConfig.RDSConfig.DBPrefix, awsSession)
			if err != nil {
				logSess.Error("create-subnet-group", err)
			}
			Expect(err).ToNot(HaveOccurred())
			logSess.Info("subnet-group-created", lager.Data{"name": rdsSubnetGroupName})

			logSess.Info("create-security-group")
			ec2SecurityGroupID, err = CreateSecurityGroup(rdsBrokerConfig.RDSConfig.DBPrefix, awsSession)
			if err != nil {
				logSess.Error("create-security-group", err)
			}
			Expect(err).ToNot(HaveOccurred())
			logSess.Info("security-group-created", lager.Data{"id": ec2SecurityGroupID})

			for serviceIndex := range rdsBrokerConfig.RDSConfig.Catalog.Services {
				for planIndex := range rdsBrokerConfig.RDSConfig.Catalog.Services[serviceIndex].Plans {
					plan := &rdsBrokerConfig.RDSConfig.Catalog.Services[serviceIndex].Plans[planIndex]
					plan.RDSProperties.DBSubnetGroupName = rdsSubnetGroupName
					plan.RDSProperties.VpcSecurityGroupIds = []*string{ec2SecurityGroupID}
				}
			}

			suiteData = SuiteData{
				RdsBrokerPath:   rdsBrokerPath,
				RdsBrokerConfig: rdsBrokerConfig,
			}

			var data bytes.Buffer
			err = gob.NewEncoder(&data).Encode(suiteData)
			Expect(err).ToNot(HaveOccurred())
			return data.Bytes()
		},
		func(data []byte) {
			err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&suiteData)
			Expect(err).ToNot(HaveOccurred())
		},
	)

	SynchronizedAfterSuite(
		func() {},
		func() {
			logSess := testSuiteLogger.Session("after-suite")
			awsSession := session.New(&aws.Config{
				Region: aws.String(suiteData.RdsBrokerConfig.RDSConfig.Region)},
			)

			logSess.Info("remove-databases")
			deletedDbIds, err := CleanUpTestDatabaseInstances(suiteData.RdsBrokerConfig.RDSConfig.DBPrefix, awsSession, logSess)

			if err != nil {
				logSess.Error("remove-databases", err)
			}
			Expect(err).ToNot(HaveOccurred())

			logSess.Info("wait-for-db-deletion")
			err = WaitForDatabasesToBeDeleted(deletedDbIds, awsSession, logSess)
			if err != nil {
				logSess.Error("wait-for-db-deletion", err)
			}
			Expect(err).ToNot(HaveOccurred())

			if ec2SecurityGroupID != nil {
				logSess.Info("remove-security-group")
				err = DestroySecurityGroup(ec2SecurityGroupID, awsSession, logSess)
				if err != nil {
					logSess.Error("remove-security-group", err)
				}
				Expect(err).To(Succeed())
			}
			if rdsSubnetGroupName != nil {
				logSess.Info("remove-subnet-group")
				err = DestroySubnetGroup(rdsSubnetGroupName, awsSession, logSess)
				if err != nil {
					logSess.Error("remove-subnet-group", err)
				}
				Expect(err).To(Succeed())
			}

			logSess.Info("remove-parameter-groups")
			err = CleanUpParameterGroups(suiteData.RdsBrokerConfig.RDSConfig.DBPrefix, awsSession, logSess)
			if err != nil {
				logSess.Error("remove-parameter-groups", err)
			}
			Expect(err).ToNot(HaveOccurred())
		},
	)

	RegisterFailHandler(Fail)
	RunSpecs(t, "RDS Broker Integration Suite")
}
