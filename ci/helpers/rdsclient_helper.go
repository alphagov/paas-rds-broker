package helpers

import (
	"fmt"
	"strings"
	"time"

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
		region:   region,
		dbPrefix: dbPrefix,
		rdssvc:   rdssvc,
	}, nil
}

func (b *RDSClient) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", strings.Replace(b.dbPrefix, "_", "-", -1), strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSClient) dbInstanceIdentifierToServiceInstanceID(serviceInstanceID string) string {
	return strings.TrimPrefix(serviceInstanceID, strings.Replace(b.dbPrefix, "_", "-", -1)+"-")
}

func (b *RDSClient) DBInstanceFinalSnapshotIdentifier(instanceID string) string {
	return b.dbInstanceIdentifier(instanceID) + "-final-snapshot"
}

func (r *RDSClient) Ping() (bool, error) {
	params := &rds.DescribeDBEngineVersionsInput{}

	_, err := r.rdssvc.DescribeDBEngineVersions(params)

	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RDSClient) CreateDBSnapshot(ID string) (string, error) {
	snapshotID := r.dbInstanceIdentifier(ID) + time.Now().Format("2006-01-02-15-04")

	params := &rds.CreateDBSnapshotInput{
		DBInstanceIdentifier: aws.String(r.dbInstanceIdentifier(ID)),
		DBSnapshotIdentifier: aws.String(snapshotID),
	}

	_, err := r.rdssvc.CreateDBSnapshot(params)

	if err != nil {
		return snapshotID, err
	}
	return snapshotID, nil
}

func (r *RDSClient) GetDBSnapshot(snapshotID string) (*rds.DescribeDBSnapshotsOutput, error) {
	params := &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String(snapshotID),
	}

	resp, err := r.rdssvc.DescribeDBSnapshots(params)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *RDSClient) DeleteDBSnapshot(snapshotID string) (*rds.DeleteDBSnapshotOutput, error) {
	params := &rds.DeleteDBSnapshotInput{
		DBSnapshotIdentifier: aws.String(snapshotID),
	}

	resp, err := r.rdssvc.DeleteDBSnapshot(params)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *RDSClient) GetDBInstanceDetails(ID string) (*rds.DescribeDBInstancesOutput, error) {
	params := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(r.dbInstanceIdentifier(ID)),
	}

	return r.rdssvc.DescribeDBInstances(params)
}
