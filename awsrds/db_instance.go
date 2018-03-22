package awsrds

import (
	"errors"
	"time"
)

type DBInstance interface {
	Describe(ID string) (DBInstanceDetails, error)
	DescribeByTag(TagName, TagValue string) ([]*DBInstanceDetails, error)
	DescribeSnapshots(DBInstanceID string) ([]*DBSnapshotDetails, error)
	DeleteSnapshots(brokerName string, keepForDays int) error
	Create(ID string, dbInstanceDetails DBInstanceDetails) error
	Restore(ID, snapshotIdentifier string, dbInstanceDetails DBInstanceDetails) error
	Modify(ID string, dbInstanceDetails DBInstanceDetails, applyImmediately bool) error
	Reboot(ID string) error
	RemoveTag(ID, tagKey string) error
	Delete(ID string, skipFinalSnapshot bool) error
	GetTag(ID, tagKey string) (string, error)
}

type DBInstanceDetails struct {
	Identifier                 string
	Status                     string
	DBInstanceClass            string
	Engine                     string
	EngineVersion              string
	Address                    string
	AllocatedStorage           int64
	Arn                        string
	AutoMinorVersionUpgrade    bool
	AvailabilityZone           string
	BackupRetentionPeriod      int64
	CharacterSetName           string
	CopyTagsToSnapshot         bool
	DBName                     string
	DBParameterGroupName       string
	DBSecurityGroups           []string
	DBSubnetGroupName          string
	Iops                       int64
	KmsKeyID                   string
	LicenseModel               string
	MasterUsername             string
	MasterUserPassword         string
	MultiAZ                    bool
	OptionGroupName            string
	PendingModifications       bool
	Port                       int64
	PreferredBackupWindow      string
	PreferredMaintenanceWindow string
	PubliclyAccessible         bool
	StorageEncrypted           bool
	StorageType                string
	Tags                       map[string]string
	VpcSecurityGroupIds        []string
}

type DBSnapshotDetails struct {
	Identifier         string
	InstanceIdentifier string
	Arn                string
	CreateTime         time.Time
	Tags               map[string]string
}

type ByCreateTime []*DBSnapshotDetails

func (ct ByCreateTime) Len() int           { return len(ct) }
func (ct ByCreateTime) Swap(i, j int)      { ct[i], ct[j] = ct[j], ct[i] }
func (ct ByCreateTime) Less(i, j int) bool { return ct[i].CreateTime.After(ct[j].CreateTime) }

var (
	ErrDBInstanceDoesNotExist = errors.New("rds db instance does not exist")
)
