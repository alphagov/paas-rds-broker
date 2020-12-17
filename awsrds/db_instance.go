package awsrds

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
)

type DescribeOption string

const (
	// Should Describe* ops invalidate and refresh the cache
	DescribeRefreshCacheOption DescribeOption = "refreshCache"
)

//go:generate counterfeiter -o fakes/fake_rds_instance.go . RDSInstance
type RDSInstance interface {
	Describe(ID string) (*rds.DBInstance, error)
	GetResourceTags(resourceArn string, opts ...DescribeOption) ([]*rds.Tag, error)
	DescribeByTag(TagName, TagValue string, opts ...DescribeOption) ([]*rds.DBInstance, error)
	DescribeSnapshots(DBInstanceID string) ([]*rds.DBSnapshot, error)
	DeleteSnapshots(brokerName string, keepForDays int) error
	Create(createDBInstanceInput *rds.CreateDBInstanceInput) error
	Restore(restoreRBInstanceInput *rds.RestoreDBInstanceFromDBSnapshotInput) error
	RestoreToPointInTime(restoreRBInstanceInput *rds.RestoreDBInstanceToPointInTimeInput) error
	Modify(modifyDBInstanceInput *rds.ModifyDBInstanceInput) (*rds.DBInstance, error)
	AddTagsToResource(resourceArn string, tags []*rds.Tag) error
	Reboot(rebootDBInstanceInput *rds.RebootDBInstanceInput) error
	RemoveTag(ID, tagKey string) error
	Delete(ID string, skipFinalSnapshot bool) error
	GetTag(ID, tagKey string) (string, error)
	GetParameterGroup(groupId string) (*rds.DBParameterGroup, error)
	CreateParameterGroup(input *rds.CreateDBParameterGroupInput) error
	ModifyParameterGroup(input *rds.ModifyDBParameterGroupInput) error
	GetLatestMinorVersion(engine string, version string) (*string, error)
	GetFullValidTargetVersion(engine string, currentVersion string, targetVersion string) (string, error)
}

type ByCreateTime []*rds.DBSnapshot

func (ct ByCreateTime) Len() int      { return len(ct) }
func (ct ByCreateTime) Swap(i, j int) { ct[i], ct[j] = ct[j], ct[i] }
func (ct ByCreateTime) Less(i, j int) bool {
	return aws.TimeValue(ct[i].SnapshotCreateTime).After(aws.TimeValue(ct[j].SnapshotCreateTime))
}

var (
	ErrDBInstanceDoesNotExist = errors.New("rds db instance does not exist")
)
