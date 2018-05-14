package integration_aws_test

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	uuid "github.com/satori/go.uuid"

	"github.com/alphagov/paas-rds-broker/config"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
)

var (
	rdsBrokerPath   string
	rdsBrokerConfig *config.Config

	rdsSubnetGroupName *string
	ec2SecurityGroupID *string
)

func TestSuite(t *testing.T) {
	BeforeSuite(func() {
		var err error

		// Compile the broker
		rdsBrokerPath, err = gexec.Build("github.com/alphagov/paas-rds-broker")
		Expect(err).ShouldNot(HaveOccurred())

		// Update config
		rdsBrokerConfig, err = config.LoadConfig("./config.json")
		Expect(err).ToNot(HaveOccurred())
		err = rdsBrokerConfig.Validate()
		Expect(err).ToNot(HaveOccurred())

		rdsBrokerConfig.RDSConfig.BrokerName = fmt.Sprintf("%s-%s",
			rdsBrokerConfig.RDSConfig.BrokerName,
			uuid.NewV4().String(),
		)

		awsSession := session.New(&aws.Config{
			Region: aws.String(rdsBrokerConfig.RDSConfig.Region)},
		)
		rdsSubnetGroupName, err = CreateSubnetGroup(rdsBrokerConfig.RDSConfig.DBPrefix, awsSession)
		Expect(err).ToNot(HaveOccurred())
		ec2SecurityGroupID, err = CreateSecurityGroup(rdsBrokerConfig.RDSConfig.DBPrefix, awsSession)
		Expect(err).ToNot(HaveOccurred())

		for serviceIndex := range rdsBrokerConfig.RDSConfig.Catalog.Services {
			for planIndex := range rdsBrokerConfig.RDSConfig.Catalog.Services[serviceIndex].Plans {
				plan := &rdsBrokerConfig.RDSConfig.Catalog.Services[serviceIndex].Plans[planIndex]
				plan.RDSProperties.DBSubnetGroupName = *rdsSubnetGroupName
				plan.RDSProperties.VpcSecurityGroupIds = []string{*ec2SecurityGroupID}
			}
		}
	})

	AfterSuite(func() {
		awsSession := session.New(&aws.Config{
			Region: aws.String(rdsBrokerConfig.RDSConfig.Region)},
		)
		if ec2SecurityGroupID != nil {
			Expect(DestroySecurityGroup(ec2SecurityGroupID, awsSession)).To(Succeed())
		}
		if rdsSubnetGroupName != nil {
			Expect(DestroySubnetGroup(rdsSubnetGroupName, awsSession)).To(Succeed())
		}
	})

	RegisterFailHandler(Fail)
	RunSpecs(t, "RDS Broker Integration Suite")
}
