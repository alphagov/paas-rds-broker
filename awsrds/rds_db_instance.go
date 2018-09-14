package awsrds

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
)

const (
	TagServiceID            = "Service ID"
	TagPlanID               = "Plan ID"
	TagOrganizationID       = "Organization ID"
	TagSpaceID              = "Space ID"
	TagSkipFinalSnapshot    = "SkipFinalSnapshot"
	TagRestoredFromSnapshot = "Restored From Snapshot"
	TagBrokerName           = "Broker Name"
)

type RDSDBInstance struct {
	region         string
	partition      string
	rdssvc         *rds.RDS
	cachedTags     map[string][]*rds.Tag
	cachedTagsLock sync.RWMutex
	logger         lager.Logger
}

func NewRDSDBInstance(
	region string,
	partition string,
	rdssvc *rds.RDS,
	logger lager.Logger,
) *RDSDBInstance {
	return &RDSDBInstance{
		region:     region,
		partition:  partition,
		rdssvc:     rdssvc,
		cachedTags: map[string][]*rds.Tag{},
		logger:     logger.Session("db-instance"),
	}
}

func (r *RDSDBInstance) Describe(ID string, opts ...DescribeOption) (DBInstanceWithTags, error) {
	var dbInstanceWithTags = DBInstanceWithTags{}
	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(ID),
	}

	refreshCache := false
	for _, o := range opts {
		if o == DescribeRefreshCacheOption {
			refreshCache = true
		}
	}

	r.logger.Debug("describe-db-instances", lager.Data{"input": describeDBInstancesInput})

	dbInstances, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		return dbInstanceWithTags, HandleAWSError(err, r.logger)
	}

	for _, dbInstance := range dbInstances.DBInstances {
		if aws.StringValue(dbInstance.DBInstanceIdentifier) == ID {
			dbInstanceWithTags.DBInstance = dbInstance
			r.logger.Debug("describe-db-instances", lager.Data{"db-instance": dbInstance})
			t, err := r.cachedListTagsForResource(*dbInstance.DBInstanceArn, refreshCache)
			if err != nil {
				return dbInstanceWithTags, HandleAWSError(err, r.logger)
			}
			dbInstanceWithTags.Tags = t
			return dbInstanceWithTags, nil
		}
	}

	return dbInstanceWithTags, ErrDBInstanceDoesNotExist
}

func (r *RDSDBInstance) DescribeByTag(tagKey, tagValue string, opts ...DescribeOption) ([]*DBInstanceDetails, error) {
	dbInstanceDetails := []*DBInstanceDetails{}

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{}

	refreshCache := false
	for _, o := range opts {
		if o == DescribeRefreshCacheOption {
			refreshCache = true
		}
	}

	var dbInstances []*rds.DBInstance
	err := r.rdssvc.DescribeDBInstancesPages(describeDBInstancesInput,
		func(page *rds.DescribeDBInstancesOutput, lastPage bool) bool {
			dbInstances = append(dbInstances, page.DBInstances...)
			return true
		},
	)

	if err != nil {
		return dbInstanceDetails, err
	}
	for _, dbInstance := range dbInstances {
		tags, err := r.cachedListTagsForResource(*dbInstance.DBInstanceArn, refreshCache)
		if err != nil {
			return dbInstanceDetails, err
		}
		for _, t := range tags {
			if *t.Key == tagKey && *t.Value == tagValue {
				d := r.buildDBInstance(dbInstance)
				dbInstanceDetails = append(dbInstanceDetails, &d)
				break
			}
		}
	}

	return dbInstanceDetails, nil
}

func (r *RDSDBInstance) DescribeSnapshots(DBInstanceID string) ([]*DBSnapshotDetails, error) {
	dbSnapshotDetails := []*DBSnapshotDetails{}

	describeDBSnapshotsInput := &rds.DescribeDBSnapshotsInput{
		DBInstanceIdentifier: aws.String(DBInstanceID),
	}

	r.logger.Debug("describe-db-snapshots", lager.Data{"input": describeDBSnapshotsInput})

	describeDBSnapshotsOutput, err := r.rdssvc.DescribeDBSnapshots(describeDBSnapshotsInput)
	if err != nil {
		return dbSnapshotDetails, HandleAWSError(err, r.logger)
	}

	for _, dbSnapshot := range describeDBSnapshotsOutput.DBSnapshots {
		s := r.buildDBSnapshot(dbSnapshot)
		if err != nil {
			return dbSnapshotDetails, HandleAWSError(err, r.logger)
		}
		t, err := ListTagsForResource(aws.StringValue(dbSnapshot.DBSnapshotArn), r.rdssvc, r.logger)
		if err != nil {
			return dbSnapshotDetails, HandleAWSError(err, r.logger)
		}
		s.Tags = RDSTagsValues(t)
		dbSnapshotDetails = append(dbSnapshotDetails, &s)
	}

	sort.Sort(ByCreateTime(dbSnapshotDetails))
	return dbSnapshotDetails, nil
}

func (r *RDSDBInstance) DeleteSnapshots(brokerName string, keepForDays int) error {
	r.logger.Info("delete-snapshots", lager.Data{"broker_name": brokerName, "keep_for_days": keepForDays})

	deleteBefore := time.Now().Add(-1 * time.Duration(keepForDays) * 24 * time.Hour)

	oldSnapshots := []*rds.DBSnapshot{}

	err := r.rdssvc.DescribeDBSnapshotsPages(
		&rds.DescribeDBSnapshotsInput{
			SnapshotType: aws.String("manual"),
		},
		func(page *rds.DescribeDBSnapshotsOutput, lastPage bool) bool {
			for _, snapshot := range page.DBSnapshots {
				if snapshot.SnapshotCreateTime.Before(deleteBefore) {
					oldSnapshots = append(oldSnapshots, snapshot)
				}
			}
			return true
		},
	)
	if err != nil {
		return fmt.Errorf("failed to fetch snapshot list from AWS API: %s", err)
	}

	snapshotsToDelete := []string{}
	for _, snapshot := range oldSnapshots {
		tags, err := ListTagsForResource(*snapshot.DBSnapshotArn, r.rdssvc, r.logger)
		if err != nil {
			return fmt.Errorf("failed to list tags for %s: %s", *snapshot.DBSnapshotIdentifier, err)
		}
		for _, tag := range tags {
			if *tag.Key == TagBrokerName && *tag.Value == brokerName {
				snapshotsToDelete = append(snapshotsToDelete, *snapshot.DBSnapshotIdentifier)
			}
		}
	}

	if len(snapshotsToDelete) > 0 {
		for _, snapshotID := range snapshotsToDelete {
			r.logger.Info("delete-snapshot", lager.Data{"snapshot_id": snapshotID})
			_, err := r.rdssvc.DeleteDBSnapshot(&rds.DeleteDBSnapshotInput{
				DBSnapshotIdentifier: &snapshotID,
			})
			if err != nil {
				return fmt.Errorf("failed to delete %s: %s", snapshotID, err)
			}
		}
	}

	return nil
}

func (r *RDSDBInstance) GetTag(ID, tagKey string) (string, error) {

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(ID),
	}

	r.logger.Debug("get-tag", lager.Data{"input": describeDBInstancesInput})

	myInstance, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		return "", HandleAWSError(err, r.logger)
	}

	listTagsForResourceInput := &rds.ListTagsForResourceInput{
		ResourceName: myInstance.DBInstances[0].DBInstanceArn,
	}

	listTagsForResourceOutput, err := r.rdssvc.ListTagsForResource(listTagsForResourceInput)
	if err != nil {
		return "", err
	}

	for _, t := range listTagsForResourceOutput.TagList {
		if *t.Key == tagKey {
			return *t.Value, nil
		}
	}

	return "", nil
}

func (r *RDSDBInstance) Create(ID string, dbInstanceDetails DBInstanceDetails) error {
	createDBInstanceInput := r.buildCreateDBInstanceInput(ID, dbInstanceDetails)

	sanitizedDBInstanceInput := *createDBInstanceInput
	sanitizedDBInstanceInput.MasterUserPassword = aws.String("REDACTED")
	r.logger.Debug("create-db-instance", lager.Data{"input": &sanitizedDBInstanceInput})

	createDBInstanceOutput, err := r.rdssvc.CreateDBInstance(createDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}
	r.logger.Debug("create-db-instance", lager.Data{"output": createDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) Restore(ID, snapshotIdentifier string, dbInstanceDetails DBInstanceDetails) error {
	restoreDBInstanceInput := r.buildRestoreDBInstanceInput(ID, snapshotIdentifier, dbInstanceDetails)
	r.logger.Debug("restore-db-instance", lager.Data{"input": &restoreDBInstanceInput})

	restoreDBInstanceOutput, err := r.rdssvc.RestoreDBInstanceFromDBSnapshot(restoreDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}
	r.logger.Debug("restore-db-instance", lager.Data{"output": restoreDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) Modify(ID string, dbInstanceDetails DBInstanceDetails, applyImmediately bool) error {
	oldDBInstanceWithTags, err := r.Describe(ID)
	if err != nil {
		return err
	}

	if dbInstanceDetails.Engine != "" && strings.ToLower(aws.StringValue(oldDBInstanceWithTags.Engine)) != strings.ToLower(dbInstanceDetails.Engine) {
		return fmt.Errorf("Migrating the RDS DB Instance engine from '%s' to '%s' is not supported", aws.StringValue(oldDBInstanceWithTags.Engine), dbInstanceDetails.Engine)
	}

	modifyDBInstanceInput := r.buildModifyDBInstanceInput(ID, dbInstanceDetails, oldDBInstanceWithTags, applyImmediately)

	sanitizedDBInstanceInput := *modifyDBInstanceInput
	sanitizedDBInstanceInput.MasterUserPassword = aws.String("REDACTED")
	r.logger.Debug("modify-db-instance", lager.Data{"input": &sanitizedDBInstanceInput})

	modifyDBInstanceOutput, err := r.rdssvc.ModifyDBInstance(modifyDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("modify-db-instance", lager.Data{"output": modifyDBInstanceOutput})

	if len(dbInstanceDetails.Tags) > 0 {
		tags := BuilRDSTags(dbInstanceDetails.Tags)
		AddTagsToResource(aws.StringValue(oldDBInstanceWithTags.DBInstanceArn), tags, r.rdssvc, r.logger)
	}

	return nil
}

func (r *RDSDBInstance) Reboot(ID string) error {
	rebootDBInstanceInput := &rds.RebootDBInstanceInput{
		DBInstanceIdentifier: aws.String(ID),
	}

	r.logger.Debug("reboot-db-instance", lager.Data{"input": rebootDBInstanceInput})

	rebootDBInstanceOutput, err := r.rdssvc.RebootDBInstance(rebootDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("reboot-db-instance", lager.Data{"output": rebootDBInstanceOutput})
	return nil
}

func (r *RDSDBInstance) RemoveTag(ID, tagKey string) error {
	dbInstanceWithTags, err := r.Describe(ID)
	if err != nil {
		return err
	}

	return RemoveTagsFromResource(aws.StringValue(dbInstanceWithTags.DBInstanceArn), []*string{&tagKey}, r.rdssvc, r.logger)
}

func (r *RDSDBInstance) Delete(ID string, skipFinalSnapshot bool) error {
	deleteDBInstanceInput := r.buildDeleteDBInstanceInput(ID, skipFinalSnapshot)
	r.logger.Debug("delete-db-instance", lager.Data{"input": deleteDBInstanceInput})

	deleteDBInstanceOutput, err := r.rdssvc.DeleteDBInstance(deleteDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("delete-db-instance", lager.Data{"output": deleteDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) buildDBInstance(dbInstance *rds.DBInstance) DBInstanceDetails {
	dbInstanceDetails := DBInstanceDetails{
		Identifier:              aws.StringValue(dbInstance.DBInstanceIdentifier),
		Arn:                     aws.StringValue(dbInstance.DBInstanceArn),
		Status:                  aws.StringValue(dbInstance.DBInstanceStatus),
		Engine:                  aws.StringValue(dbInstance.Engine),
		EngineVersion:           aws.StringValue(dbInstance.EngineVersion),
		DBName:                  aws.StringValue(dbInstance.DBName),
		MasterUsername:          aws.StringValue(dbInstance.MasterUsername),
		AllocatedStorage:        aws.Int64Value(dbInstance.AllocatedStorage),
		AutoMinorVersionUpgrade: aws.BoolValue(dbInstance.AutoMinorVersionUpgrade),
		BackupRetentionPeriod:   aws.Int64Value(dbInstance.BackupRetentionPeriod),
		CopyTagsToSnapshot:      aws.BoolValue(dbInstance.CopyTagsToSnapshot),
		MultiAZ:                 aws.BoolValue(dbInstance.MultiAZ),
		PubliclyAccessible:      aws.BoolValue(dbInstance.PubliclyAccessible),
		StorageEncrypted:        aws.BoolValue(dbInstance.StorageEncrypted),
	}

	if dbInstance.Endpoint != nil {
		dbInstanceDetails.Address = aws.StringValue(dbInstance.Endpoint.Address)
		dbInstanceDetails.Port = aws.Int64Value(dbInstance.Endpoint.Port)
	}

	if dbInstance.PendingModifiedValues != nil {
		emptyPendingModifiedValues := &rds.PendingModifiedValues{}
		if !reflect.DeepEqual(*dbInstance.PendingModifiedValues, *emptyPendingModifiedValues) {
			dbInstanceDetails.PendingModifications = true
		}
	}

	return dbInstanceDetails
}

func (r *RDSDBInstance) buildDBSnapshot(dbSnapshot *rds.DBSnapshot) DBSnapshotDetails {
	dbSnapshotDetails := DBSnapshotDetails{
		Identifier:         aws.StringValue(dbSnapshot.DBSnapshotIdentifier),
		InstanceIdentifier: aws.StringValue(dbSnapshot.DBInstanceIdentifier),
		CreateTime:         aws.TimeValue(dbSnapshot.SnapshotCreateTime),
	}
	return dbSnapshotDetails
}

func (r *RDSDBInstance) buildCreateDBInstanceInput(ID string, dbInstanceDetails DBInstanceDetails) *rds.CreateDBInstanceInput {
	createDBInstanceInput := &rds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(ID),
		Engine:               aws.String(dbInstanceDetails.Engine),
	}

	if dbInstanceDetails.AllocatedStorage > 0 {
		createDBInstanceInput.AllocatedStorage = aws.Int64(dbInstanceDetails.AllocatedStorage)
	}

	createDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(dbInstanceDetails.AutoMinorVersionUpgrade)

	if dbInstanceDetails.AvailabilityZone != "" {
		createDBInstanceInput.AvailabilityZone = aws.String(dbInstanceDetails.AvailabilityZone)
	}

	createDBInstanceInput.BackupRetentionPeriod = aws.Int64(dbInstanceDetails.BackupRetentionPeriod)

	if dbInstanceDetails.CharacterSetName != "" {
		createDBInstanceInput.CharacterSetName = aws.String(dbInstanceDetails.CharacterSetName)
	}

	createDBInstanceInput.CopyTagsToSnapshot = aws.Bool(dbInstanceDetails.CopyTagsToSnapshot)

	if dbInstanceDetails.DBInstanceClass != "" {
		createDBInstanceInput.DBInstanceClass = aws.String(dbInstanceDetails.DBInstanceClass)
	}

	if dbInstanceDetails.DBName != "" {
		createDBInstanceInput.DBName = aws.String(dbInstanceDetails.DBName)
	}

	if dbInstanceDetails.DBParameterGroupName != "" {
		createDBInstanceInput.DBParameterGroupName = aws.String(dbInstanceDetails.DBParameterGroupName)
	}

	if len(dbInstanceDetails.DBSecurityGroups) > 0 {
		createDBInstanceInput.DBSecurityGroups = aws.StringSlice(dbInstanceDetails.DBSecurityGroups)
	}

	if dbInstanceDetails.DBSubnetGroupName != "" {
		createDBInstanceInput.DBSubnetGroupName = aws.String(dbInstanceDetails.DBSubnetGroupName)
	}

	if dbInstanceDetails.EngineVersion != "" {
		createDBInstanceInput.EngineVersion = aws.String(dbInstanceDetails.EngineVersion)
	}

	if dbInstanceDetails.KmsKeyID != "" {
		createDBInstanceInput.KmsKeyId = aws.String(dbInstanceDetails.KmsKeyID)
	}

	if dbInstanceDetails.LicenseModel != "" {
		createDBInstanceInput.LicenseModel = aws.String(dbInstanceDetails.LicenseModel)
	}

	if dbInstanceDetails.MasterUsername != "" {
		createDBInstanceInput.MasterUsername = aws.String(dbInstanceDetails.MasterUsername)
	}

	if dbInstanceDetails.MasterUserPassword != "" {
		createDBInstanceInput.MasterUserPassword = aws.String(dbInstanceDetails.MasterUserPassword)
	}

	createDBInstanceInput.MultiAZ = aws.Bool(dbInstanceDetails.MultiAZ)

	if dbInstanceDetails.OptionGroupName != "" {
		createDBInstanceInput.OptionGroupName = aws.String(dbInstanceDetails.OptionGroupName)
	}

	if dbInstanceDetails.Port > 0 {
		createDBInstanceInput.Port = aws.Int64(dbInstanceDetails.Port)
	}

	if dbInstanceDetails.PreferredBackupWindow != "" {
		createDBInstanceInput.PreferredBackupWindow = aws.String(dbInstanceDetails.PreferredBackupWindow)
	}

	if dbInstanceDetails.PreferredMaintenanceWindow != "" {
		createDBInstanceInput.PreferredMaintenanceWindow = aws.String(dbInstanceDetails.PreferredMaintenanceWindow)
	}

	createDBInstanceInput.PubliclyAccessible = aws.Bool(dbInstanceDetails.PubliclyAccessible)

	createDBInstanceInput.StorageEncrypted = aws.Bool(dbInstanceDetails.StorageEncrypted)

	if dbInstanceDetails.StorageType != "" {
		createDBInstanceInput.StorageType = aws.String(dbInstanceDetails.StorageType)
	}

	if dbInstanceDetails.Iops > 0 {
		createDBInstanceInput.Iops = aws.Int64(dbInstanceDetails.Iops)
	}

	if len(dbInstanceDetails.VpcSecurityGroupIds) > 0 {
		createDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice(dbInstanceDetails.VpcSecurityGroupIds)
	}

	if len(dbInstanceDetails.Tags) > 0 {
		createDBInstanceInput.Tags = BuilRDSTags(dbInstanceDetails.Tags)
	}

	return createDBInstanceInput
}

func (r *RDSDBInstance) buildRestoreDBInstanceInput(ID, snapshotIdentifier string, dbInstanceDetails DBInstanceDetails) *rds.RestoreDBInstanceFromDBSnapshotInput {
	restoreDBInstanceInput := &rds.RestoreDBInstanceFromDBSnapshotInput{
		DBInstanceIdentifier: aws.String(ID),
		DBSnapshotIdentifier: aws.String(snapshotIdentifier),
		Engine:               aws.String(dbInstanceDetails.Engine),
	}

	restoreDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(dbInstanceDetails.AutoMinorVersionUpgrade)

	if dbInstanceDetails.AvailabilityZone != "" {
		restoreDBInstanceInput.AvailabilityZone = aws.String(dbInstanceDetails.AvailabilityZone)
	}

	restoreDBInstanceInput.CopyTagsToSnapshot = aws.Bool(dbInstanceDetails.CopyTagsToSnapshot)

	if dbInstanceDetails.DBInstanceClass != "" {
		restoreDBInstanceInput.DBInstanceClass = aws.String(dbInstanceDetails.DBInstanceClass)
	}

	if dbInstanceDetails.DBName != "" {
		restoreDBInstanceInput.DBName = aws.String(dbInstanceDetails.DBName)
	}

	if dbInstanceDetails.DBSubnetGroupName != "" {
		restoreDBInstanceInput.DBSubnetGroupName = aws.String(dbInstanceDetails.DBSubnetGroupName)
	}

	if dbInstanceDetails.LicenseModel != "" {
		restoreDBInstanceInput.LicenseModel = aws.String(dbInstanceDetails.LicenseModel)
	}

	restoreDBInstanceInput.MultiAZ = aws.Bool(dbInstanceDetails.MultiAZ)

	if dbInstanceDetails.OptionGroupName != "" {
		restoreDBInstanceInput.OptionGroupName = aws.String(dbInstanceDetails.OptionGroupName)
	}

	if dbInstanceDetails.Port > 0 {
		restoreDBInstanceInput.Port = aws.Int64(dbInstanceDetails.Port)
	}

	restoreDBInstanceInput.PubliclyAccessible = aws.Bool(dbInstanceDetails.PubliclyAccessible)

	if dbInstanceDetails.StorageType != "" {
		restoreDBInstanceInput.StorageType = aws.String(dbInstanceDetails.StorageType)
	}

	if dbInstanceDetails.Iops > 0 {
		restoreDBInstanceInput.Iops = aws.Int64(dbInstanceDetails.Iops)
	}

	if len(dbInstanceDetails.Tags) > 0 {
		restoreDBInstanceInput.Tags = BuilRDSTags(dbInstanceDetails.Tags)
	}

	return restoreDBInstanceInput
}

func (r *RDSDBInstance) buildModifyDBInstanceInput(ID string, dbInstanceDetails DBInstanceDetails, oldDBInstanceWithTags DBInstanceWithTags, applyImmediately bool) *rds.ModifyDBInstanceInput {
	modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String(ID),
		ApplyImmediately:     aws.Bool(applyImmediately),
	}

	if dbInstanceDetails.AllocatedStorage > 0 {
		if dbInstanceDetails.AllocatedStorage < aws.Int64Value(oldDBInstanceWithTags.AllocatedStorage) {
			modifyDBInstanceInput.AllocatedStorage = oldDBInstanceWithTags.AllocatedStorage
		} else {
			modifyDBInstanceInput.AllocatedStorage = aws.Int64(dbInstanceDetails.AllocatedStorage)
		}
	}

	modifyDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(dbInstanceDetails.AutoMinorVersionUpgrade)

	if dbInstanceDetails.BackupRetentionPeriod > 0 {
		modifyDBInstanceInput.BackupRetentionPeriod = aws.Int64(dbInstanceDetails.BackupRetentionPeriod)
	}

	modifyDBInstanceInput.CopyTagsToSnapshot = aws.Bool(dbInstanceDetails.CopyTagsToSnapshot)

	if dbInstanceDetails.DBInstanceClass != "" {
		modifyDBInstanceInput.DBInstanceClass = aws.String(dbInstanceDetails.DBInstanceClass)
	}

	if dbInstanceDetails.DBParameterGroupName != "" {
		modifyDBInstanceInput.DBParameterGroupName = aws.String(dbInstanceDetails.DBParameterGroupName)
	}

	if len(dbInstanceDetails.DBSecurityGroups) > 0 {
		modifyDBInstanceInput.DBSecurityGroups = aws.StringSlice(dbInstanceDetails.DBSecurityGroups)
	}

	if dbInstanceDetails.EngineVersion != "" && dbInstanceDetails.EngineVersion != aws.StringValue(oldDBInstanceWithTags.EngineVersion) {
		modifyDBInstanceInput.EngineVersion = aws.String(dbInstanceDetails.EngineVersion)
		modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(r.allowMajorVersionUpgrade(dbInstanceDetails.EngineVersion, aws.StringValue(oldDBInstanceWithTags.EngineVersion)))
	}

	if dbInstanceDetails.MasterUserPassword != "" {
		modifyDBInstanceInput.MasterUserPassword = aws.String(dbInstanceDetails.MasterUserPassword)
	}

	modifyDBInstanceInput.MultiAZ = aws.Bool(dbInstanceDetails.MultiAZ)

	if dbInstanceDetails.OptionGroupName != "" {
		modifyDBInstanceInput.OptionGroupName = aws.String(dbInstanceDetails.OptionGroupName)
	}

	if dbInstanceDetails.PreferredBackupWindow != "" {
		modifyDBInstanceInput.PreferredBackupWindow = aws.String(dbInstanceDetails.PreferredBackupWindow)
	}

	if dbInstanceDetails.PreferredMaintenanceWindow != "" {
		modifyDBInstanceInput.PreferredMaintenanceWindow = aws.String(dbInstanceDetails.PreferredMaintenanceWindow)
	}

	if dbInstanceDetails.StorageType != "" {
		modifyDBInstanceInput.StorageType = aws.String(dbInstanceDetails.StorageType)
	}

	if dbInstanceDetails.Iops > 0 {
		modifyDBInstanceInput.Iops = aws.Int64(dbInstanceDetails.Iops)
	}

	if len(dbInstanceDetails.VpcSecurityGroupIds) > 0 {
		modifyDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice(dbInstanceDetails.VpcSecurityGroupIds)
	}

	return modifyDBInstanceInput
}

func (r *RDSDBInstance) buildDeleteDBInstanceInput(ID string, skipFinalSnapshot bool) *rds.DeleteDBInstanceInput {
	deleteDBInstanceInput := &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(ID),
		SkipFinalSnapshot:    aws.Bool(skipFinalSnapshot),
	}

	if !skipFinalSnapshot {
		deleteDBInstanceInput.FinalDBSnapshotIdentifier = aws.String(r.dbSnapshotName(ID))
	}

	return deleteDBInstanceInput
}

func (r *RDSDBInstance) dbSnapshotName(ID string) string {
	return fmt.Sprintf("%s-final-snapshot", ID)
}

func (r *RDSDBInstance) allowMajorVersionUpgrade(newEngineVersion, oldEngineVersion string) bool {
	newSplittedEngineVersion := strings.Split(newEngineVersion, ".")
	newMajorEngineVersion := fmt.Sprintf("%s:%s", newSplittedEngineVersion[0], newSplittedEngineVersion[1])

	oldSplittedEngineVersion := strings.Split(oldEngineVersion, ".")
	oldMajorEngineVersion := fmt.Sprintf("%s:%s", oldSplittedEngineVersion[0], oldSplittedEngineVersion[1])

	if newMajorEngineVersion > oldMajorEngineVersion {
		return true
	}

	return false
}

func (r *RDSDBInstance) cachedListTagsForResource(arn string, refresh bool) ([]*rds.Tag, error) {
	if !refresh {
		r.cachedTagsLock.RLock()
		tags, ok := r.cachedTags[arn]
		r.cachedTagsLock.RUnlock()
		if ok {
			return tags, nil
		}
	}

	tags, err := ListTagsForResource(arn, r.rdssvc, r.logger)
	if err == nil {
		r.cachedTagsLock.Lock()
		r.cachedTags[arn] = tags
		r.cachedTagsLock.Unlock()
	}
	return tags, err
}
