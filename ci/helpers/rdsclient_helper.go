package helpers

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
)

type RDSClient struct {
	region   string
	dbPrefix string
	rdssvc   *rds.RDS
}

func NewRDSClient(region string, dbPrefix string) (*RDSClient, error) {
	sess := session.New(&aws.Config{Region: aws.String(region)})
	rdssvc := rds.New(sess)
	return &RDSClient{
		region: region,
		dbPrefix: dbPrefix,
		rdssvc: rdssvc,
	}, nil
}

func (b *RDSClient) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", strings.Replace(b.dbPrefix, "_", "-", -1), strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSClient) dbInstanceIdentifierToServiceInstanceID(serviceInstanceID string) string {
	return strings.TrimPrefix(serviceInstanceID, strings.Replace(b.dbPrefix, "_", "-", -1)+"-")
}

func (r *RDSClient) Ping() (bool, error) {
	params := &rds.DescribeDBEngineVersionsInput{}

	_, err := r.rdssvc.DescribeDBEngineVersions(params)

	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RDSClient) GetDBFinalSnapshots(ID string) (*rds.DescribeDBSnapshotsOutput, error) {
	params := &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String(r.dbInstanceIdentifier(ID) + "-final-snapshot"),
	}

	resp, err := r.rdssvc.DescribeDBSnapshots(params)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *RDSClient) DeleteDBFinalSnapshot(ID string) (*rds.DeleteDBSnapshotOutput, error) {
	params := &rds.DeleteDBSnapshotInput{
		DBSnapshotIdentifier: aws.String(r.dbInstanceIdentifier(ID) + "-final-snapshot"),
	}

	resp, err := r.rdssvc.DeleteDBSnapshot(params)

	if err != nil {
		return nil, err
	}
	return resp, nil
}
