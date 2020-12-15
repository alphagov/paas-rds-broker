package helpers

import (
	"code.cloudfoundry.org/lager"
	"fmt"
	"golang.org/x/sync/errgroup"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/satori/go.uuid"
)

func CreateSubnetGroup(prefix string, session *session.Session) (*string, error) {
	vpcID, err := getNetworkMetadata("vpc-id", session)
	if err != nil {
		return nil, err
	}

	ec2Service := ec2.New(session)
	subnets, err := ec2Service.DescribeSubnets(&ec2.DescribeSubnetsInput{
		Filters: []*ec2.Filter{{
			Name:   aws.String("vpc-id"),
			Values: []*string{aws.String(vpcID)},
		}},
	})
	if err != nil {
		return nil, err
	}

	subnetIDs := make([]*string, len(subnets.Subnets))
	for i, subnet := range subnets.Subnets {
		subnetIDs[i] = subnet.SubnetId
	}

	rdsService := rds.New(session)
	subnetGroup, err := rdsService.CreateDBSubnetGroup(&rds.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String(fmt.Sprintf("%s-%s", prefix, uuid.NewV4().String())),
		DBSubnetGroupDescription: aws.String(fmt.Sprintf("%s integration tests", prefix)),
		SubnetIds:                subnetIDs,
	})
	if err != nil {
		return nil, err
	}

	return subnetGroup.DBSubnetGroup.DBSubnetGroupName, nil
}

func CreateSecurityGroup(prefix string, session *session.Session) (*string, error) {
	vpcID, err := getNetworkMetadata("vpc-id", session)
	if err != nil {
		return nil, err
	}
	localSubnet, err := getNetworkMetadata("subnet-ipv4-cidr-block", session)
	if err != nil {
		return nil, err
	}

	ec2Service := ec2.New(session)
	securityGroup, err := ec2Service.CreateSecurityGroup(&ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("%s-%s", prefix, uuid.NewV4().String())),
		Description: aws.String(fmt.Sprintf("%s integration tests", prefix)),
		VpcId:       aws.String(vpcID),
	})
	if err != nil {
		return nil, err
	}

	for _, port := range []int64{5432, 3306} {
		_, err = ec2Service.AuthorizeSecurityGroupIngress(&ec2.AuthorizeSecurityGroupIngressInput{
			GroupId: securityGroup.GroupId,
			IpPermissions: []*ec2.IpPermission{{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int64(port),
				ToPort:     aws.Int64(port),
				IpRanges:   []*ec2.IpRange{{CidrIp: aws.String(localSubnet)}},
			}},
		})
		if err != nil {
			return nil, err
		}
	}

	return securityGroup.GroupId, nil
}

func DestroySubnetGroup(name *string, session *session.Session, logger lager.Logger) error {
	logSess := logger.Session("destroy-subnet-group")
	rdsService := rds.New(session)
	logSess.Info("delete-db-subnet-group", lager.Data{"db-subnet-group-name": name})
	_, err := rdsService.DeleteDBSubnetGroup(&rds.DeleteDBSubnetGroupInput{
		DBSubnetGroupName: name,
	})

	if err != nil {
		logSess.Error("delete-db-subnet-group", err, lager.Data{"db-subnet-group-name": name})
	}

	return err
}

func DestroySecurityGroup(id *string, session *session.Session, logger lager.Logger) error {
	logSess := logger.Session("destroy-security-group")
	ec2Service := ec2.New(session)
	logSess.Info("delete-security-group", lager.Data{"group-id": id})
	_, err := ec2Service.DeleteSecurityGroup(&ec2.DeleteSecurityGroupInput{
		GroupId: id,
	})

	if err != nil {
		logSess.Error("delete-security-group", err, lager.Data{"group-id": id})
	}

	return err
}

func CleanUpParameterGroups(prefix string, session *session.Session, logger lager.Logger) error {
	logSess := logger.Session("cleanup-parameter-groups")
	if !strings.HasPrefix(prefix, "build-test-") {
		logSess.Error("cleanup-init", fmt.Errorf("trying to clean up parameter groups without the 'build-test-' prefix will fail"))
		panic("Trying to clean up parameter groups without the 'build-test-' prefix will fail")
	}

	rdsService := rds.New(session)
	parameterGroups := []string{}

	logSess.Info("list-deletable-parameter-groups")
	// Fetch all parameter group names
	err := rdsService.DescribeDBParameterGroupsPages(
		&rds.DescribeDBParameterGroupsInput{},
		func(page *rds.DescribeDBParameterGroupsOutput, lastPage bool) bool {
			for _, group := range page.DBParameterGroups {
				parameterGroups = append(parameterGroups, aws.StringValue(group.DBParameterGroupName))
			}
			return true
		},
	)

	if err != nil {
		logSess.Error("list-deletable-parameter-groups", err)
		return err
	} else {
		logSess.Info("deletable-parameter-groups", lager.Data{"parameter-group-names": parameterGroups})
	}

	// Delete any with a matching prefix
	for _, group := range parameterGroups {
		if strings.HasPrefix(group, prefix) {
			logSess.Info("delete-parameter-group", lager.Data{"name": group})
			_, err := rdsService.DeleteDBParameterGroup(&rds.DeleteDBParameterGroupInput{
				DBParameterGroupName: aws.String(group),
			})

			if err != nil {
				logSess.Error("delete-parameter-group", err, lager.Data{"name": group})
				return err
			}
		}
	}

	return nil
}

func CleanUpTestDatabaseInstances(prefix string, awsSession *session.Session, logger lager.Logger) ([]string, error) {
	logSess := logger.Session("cleanup-test-databases")
	if !strings.HasPrefix(prefix, "build-test-") {
		logSess.Error("cleanup-init", fmt.Errorf("trying to clean up databases without the 'build-test-' prefix will fail"))
		panic("Trying to clean up databases without the 'build-test-' prefix will fail")
	}

	rdsSvc := rds.New(awsSession)

	logSess.Info("list-deletable-databases")
	requiringDeletion := []string{}
	err := rdsSvc.DescribeDBInstancesPages(
		&rds.DescribeDBInstancesInput{},
		func(page *rds.DescribeDBInstancesOutput, lastPage bool) bool {
			if len(page.DBInstances) > 0 {
				for _, instance := range page.DBInstances {
					if strings.HasPrefix(aws.StringValue(instance.DBInstanceIdentifier), prefix) {
						if aws.StringValue(instance.DBInstanceStatus) != "deleting" {
							requiringDeletion = append(requiringDeletion, aws.StringValue(instance.DBInstanceIdentifier))
						}
					}
				}
			}

			return true
		})

	if err != nil {
		logSess.Error("list-deletable-databases", err)
		return []string{}, err
	} else {
		logSess.Info("deletable-databases", lager.Data{"instance-identifiers": requiringDeletion})
	}

	for _, instance := range requiringDeletion {
		logSess.Info("delete-database", lager.Data{"db-instance-identifier": instance})
		_, err := rdsSvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(instance),
			SkipFinalSnapshot:    aws.Bool(true),
		})

		if err != nil {
			logSess.Error("delete-database", err, lager.Data{"db-instance-identifier": instance})
			return []string{}, err
		}
	}

	return requiringDeletion, nil
}

func WaitForDatabasesToBeDeleted(ids []string, awsSession *session.Session, logger lager.Logger) error {
	logSess := logger.Session("wait-dor-db-deletion")
	rdsSvc := rds.New(awsSession)

	errGroup := errgroup.Group{}

	for _, instanceId := range ids {
		logSess.Info("begin-waiter", lager.Data{"instance-id": instanceId})
		errGroup.Go(func() error {
			return rdsSvc.WaitUntilDBInstanceDeleted(&rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(instanceId),
			})
		})
	}

	return errGroup.Wait()
}

func getNetworkMetadata(name string, session *session.Session) (string, error) {
	const prefix = "network/interfaces/macs"

	metaService := ec2metadata.New(session)
	// FIXME: What if there is more than one MAC?
	mac, err := metaService.GetMetadata(prefix)
	if err != nil {
		return "", err
	}

	return metaService.GetMetadata(strings.Join([]string{prefix, mac, name}, "/"))
}
