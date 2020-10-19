package awsrds

import (
	"fmt"
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
	TagExtensions           = "Extensions"
	TagOriginDatabase       = "Restored From Database"
	TagOriginPointInTime    = "Restored From Time"
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

func (r *RDSDBInstance) Describe(ID string) (*rds.DBInstance, error) {
	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(ID),
	}

	r.logger.Debug("describe-db-instances", lager.Data{"input": describeDBInstancesInput})

	dbInstances, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		return nil, HandleAWSError(err, r.logger)
	}

	for _, dbInstance := range dbInstances.DBInstances {
		if aws.StringValue(dbInstance.DBInstanceIdentifier) == ID {
			r.logger.Debug("describe-db-instances", lager.Data{"db-instance": dbInstance})
			return dbInstance, nil
		}
	}
	return nil, ErrDBInstanceDoesNotExist
}

func (r *RDSDBInstance) GetResourceTags(resourceArn string, opts ...DescribeOption) ([]*rds.Tag, error) {
	refreshCache := false
	for _, o := range opts {
		if o == DescribeRefreshCacheOption {
			refreshCache = true
		}
	}

	r.logger.Debug("get-resource-tags", lager.Data{"arn": resourceArn, "refresh-cache": refreshCache})

	t, err := r.cachedListTagsForResource(resourceArn, refreshCache)
	if err != nil {
		return nil, HandleAWSError(err, r.logger)
	}
	return t, nil
}

func (r *RDSDBInstance) DescribeByTag(tagKey, tagValue string, opts ...DescribeOption) ([]*rds.DBInstance, error) {
	alllDbInstances := []*rds.DBInstance{}

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{}

	refreshCache := false
	for _, o := range opts {
		if o == DescribeRefreshCacheOption {
			refreshCache = true
		}
	}

	err := r.rdssvc.DescribeDBInstancesPages(describeDBInstancesInput,
		func(page *rds.DescribeDBInstancesOutput, lastPage bool) bool {
			alllDbInstances = append(alllDbInstances, page.DBInstances...)
			return true
		},
	)

	if err != nil {
		return alllDbInstances, err
	}
	dbInstances := []*rds.DBInstance{}
	for _, dbInstance := range alllDbInstances {
		tags, err := r.cachedListTagsForResource(aws.StringValue(dbInstance.DBInstanceArn), refreshCache)
		if err != nil {
			return alllDbInstances, err
		}
		for _, t := range tags {
			if aws.StringValue(t.Key) == tagKey && aws.StringValue(t.Value) == tagValue {
				dbInstances = append(dbInstances, dbInstance)
				break
			}
		}
	}

	return dbInstances, nil
}

func (r *RDSDBInstance) DescribeSnapshots(DBInstanceID string) ([]*rds.DBSnapshot, error) {
	describeDBSnapshotsInput := &rds.DescribeDBSnapshotsInput{
		DBInstanceIdentifier: aws.String(DBInstanceID),
	}

	r.logger.Debug("describe-db-snapshots", lager.Data{"input": describeDBSnapshotsInput})

	describeDBSnapshotsOutput, err := r.rdssvc.DescribeDBSnapshots(describeDBSnapshotsInput)
	if err != nil {
		return nil, HandleAWSError(err, r.logger)
	}

	sort.Sort(ByCreateTime(describeDBSnapshotsOutput.DBSnapshots))

	return describeDBSnapshotsOutput.DBSnapshots, nil
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

func (r *RDSDBInstance) Create(createDBInstanceInput *rds.CreateDBInstanceInput) error {
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

func (r *RDSDBInstance) Restore(restoreDBInstanceInput *rds.RestoreDBInstanceFromDBSnapshotInput) error {
	r.logger.Debug("restore-db-instance", lager.Data{"input": &restoreDBInstanceInput})

	restoreDBInstanceOutput, err := r.rdssvc.RestoreDBInstanceFromDBSnapshot(restoreDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}
	r.logger.Debug("restore-db-instance", lager.Data{"output": restoreDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) RestoreToPointInTime(restoreDBInstanceInput *rds.RestoreDBInstanceToPointInTimeInput) error {
	r.logger.Debug("restore-db-instance-to-point-in-time", lager.Data{"input": &restoreDBInstanceInput})

	restoreDBInstanceOutput, err := r.rdssvc.RestoreDBInstanceToPointInTime(restoreDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}
	r.logger.Debug("restore-db-instance-to-point-in-time", lager.Data{"output": restoreDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) Modify(modifyDBInstanceInput *rds.ModifyDBInstanceInput) (*rds.DBInstance, error) {
	sanitizedDBInstanceInput := *modifyDBInstanceInput
	sanitizedDBInstanceInput.MasterUserPassword = aws.String("REDACTED")
	r.logger.Debug("modify-db-instance", lager.Data{"input": &sanitizedDBInstanceInput})

	updatedModifyDBInstanceInput := *modifyDBInstanceInput

	oldDbInstance, err := r.Describe(aws.StringValue(modifyDBInstanceInput.DBInstanceIdentifier))
	if err != nil {
		return nil, err
	}

	if modifyDBInstanceInput.EngineVersion != nil {
		updatedModifyDBInstanceInput.EngineVersion, err = r.selectEngineVersion(oldDbInstance.Engine, oldDbInstance.EngineVersion, modifyDBInstanceInput.EngineVersion)
		if err != nil {
			return nil, err
		}
	}

	if modifyDBInstanceInput.AllowMajorVersionUpgrade == nil {
		updatedModifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(true)
	}

	if modifyDBInstanceInput.AllocatedStorage != nil {
		newAllocatedSpace := aws.Int64Value(modifyDBInstanceInput.AllocatedStorage)
		oldAllocatedSpace := aws.Int64Value(oldDbInstance.AllocatedStorage)
		if newAllocatedSpace <= oldAllocatedSpace {
			updatedModifyDBInstanceInput.AllocatedStorage = nil
			r.logger.Info("modify-db-instance.prevented-storage-downgrade", lager.Data{"input": &sanitizedDBInstanceInput})
		}
	}

	newSubnetGroup := aws.StringValue(modifyDBInstanceInput.DBSubnetGroupName)
	oldSubnetGroup := ""
	if oldDbInstance.DBSubnetGroup != nil {
		oldSubnetGroup = aws.StringValue(oldDbInstance.DBSubnetGroup.DBSubnetGroupName)
	}
	if newSubnetGroup == oldSubnetGroup {
		updatedModifyDBInstanceInput.DBSubnetGroupName = nil
		r.logger.Info("modify-db-instance.prevented-update-same-subnetgroup", lager.Data{"input": &sanitizedDBInstanceInput})
	}

	modifyDBInstanceOutput, err := r.rdssvc.ModifyDBInstance(&updatedModifyDBInstanceInput)
	if err != nil {
		return nil, HandleAWSError(err, r.logger)
	}

	r.logger.Debug("modify-db-instance", lager.Data{"output": modifyDBInstanceOutput})

	return modifyDBInstanceOutput.DBInstance, nil
}

func (r *RDSDBInstance) AddTagsToResource(resourceARN string, tags []*rds.Tag) error {
	addTagsToResourceInput := &rds.AddTagsToResourceInput{
		ResourceName: aws.String(resourceARN),
		Tags:         tags,
	}

	r.logger.Debug("add-tags-to-resource", lager.Data{"input": addTagsToResourceInput})

	addTagsToResourceOutput, err := r.rdssvc.AddTagsToResource(addTagsToResourceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("add-tags-to-resource", lager.Data{"output": addTagsToResourceOutput})

	return nil
}

func (r *RDSDBInstance) Reboot(rebootDBInstanceInput *rds.RebootDBInstanceInput) error {
	r.logger.Debug("reboot-db-instance", lager.Data{"input": rebootDBInstanceInput})

	rebootDBInstanceOutput, err := r.rdssvc.RebootDBInstance(rebootDBInstanceInput)
	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("reboot-db-instance", lager.Data{"output": rebootDBInstanceOutput})
	return nil
}

func (r *RDSDBInstance) RemoveTag(ID, tagKey string) error {
	dbInstance, err := r.Describe(ID)
	if err != nil {
		return err
	}

	return RemoveTagsFromResource(aws.StringValue(dbInstance.DBInstanceArn), []*string{&tagKey}, r.rdssvc, r.logger)
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

func (r *RDSDBInstance) GetParameterGroup(groupId string) (*rds.DBParameterGroup, error) {
	describeDBParameterGroupsInput := &rds.DescribeDBParameterGroupsInput{
		DBParameterGroupName: aws.String(groupId),
		Filters:              nil,
		Marker:               nil,
		MaxRecords:           nil,
	}
	r.logger.Debug("get-parameter-group", lager.Data{"input": describeDBParameterGroupsInput})

	describeDBParameterGroupsOutput, err := r.rdssvc.DescribeDBParameterGroups(describeDBParameterGroupsInput)

	if err != nil {
		return nil, HandleAWSError(err, r.logger)
	}

	r.logger.Debug("get-parameter-group", lager.Data{"output": describeDBParameterGroupsOutput})

	return describeDBParameterGroupsOutput.DBParameterGroups[0], nil
}

func (r *RDSDBInstance) CreateParameterGroup(input *rds.CreateDBParameterGroupInput) error {
	r.logger.Debug("create-parameter-group", lager.Data{"input": input})

	createDBParameterGroupOutput, err := r.rdssvc.CreateDBParameterGroup(input)

	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("create-parameter-group", lager.Data{"output": createDBParameterGroupOutput})
	return nil
}

func (r *RDSDBInstance) ModifyParameterGroup(input *rds.ModifyDBParameterGroupInput) error {
	r.logger.Debug("modify-parameter-group", lager.Data{"input": input})

	modifyParameterGroupOutput, err := r.rdssvc.ModifyDBParameterGroup(input)

	if err != nil {
		return HandleAWSError(err, r.logger)
	}

	r.logger.Debug("modify-parameter-group", lager.Data{"output": modifyParameterGroupOutput})
	return nil
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

func (r *RDSDBInstance) selectEngineVersion(engine *string, oldEngineVersion *string, planEngineVersion *string) (newEngineVersion *string, err error) {
	keepEngineVersion := false

	oldEngineVersionSlice := strings.Split(aws.StringValue(oldEngineVersion), ".")
	planEngineVersionSlice := strings.Split(aws.StringValue(planEngineVersion), ".")

	if len(planEngineVersionSlice) == 1 {
		if oldEngineVersionSlice[0] == planEngineVersionSlice[0] {
			keepEngineVersion = true
		}
	} else if len(planEngineVersionSlice) == 2 {
		if oldEngineVersionSlice[0] == planEngineVersionSlice[0] &&
			oldEngineVersionSlice[1] == planEngineVersionSlice[1] {
			keepEngineVersion = true
		}
	} else if len(planEngineVersionSlice) == 3 {
		if oldEngineVersionSlice[0] == planEngineVersionSlice[0] &&
			oldEngineVersionSlice[1] == planEngineVersionSlice[1] &&
			oldEngineVersionSlice[2] == planEngineVersionSlice[2] {
			keepEngineVersion = true
		}
	}

	if keepEngineVersion {
		newEngineVersion = oldEngineVersion
		r.logger.Info("modify-db-instance.select-engine-version", lager.Data{"EngineVersion kept:": oldEngineVersion})
	} else {
		newEngineVersion = planEngineVersion
		r.logger.Info("modify-db-instance.select-engine-version", lager.Data{"EngineVersion updated:": newEngineVersion})
	}

	return newEngineVersion, err
}

func (r *RDSDBInstance) GetLatestMinorVersion(engine string, version string) (*string, error) {
	resp, err := r.rdssvc.DescribeDBEngineVersions(&rds.DescribeDBEngineVersionsInput{
		Engine:        aws.String(engine),
		EngineVersion: aws.String(version),
	})
	if err != nil {
		return nil, err
	}

	r.logger.Info(
		"get-latest-minor-version.describe",
		lager.Data{"version-count": len(resp.DBEngineVersions)},
	)

	if len(resp.DBEngineVersions) != 1 {
		return nil, fmt.Errorf("Did not find a single version for %s/%s", engine, version)
	}

	validUpgradeTargets := []rds.UpgradeTarget{}
	for _, target := range resp.DBEngineVersions[0].ValidUpgradeTarget {
		if target.IsMajorVersionUpgrade != nil && *target.IsMajorVersionUpgrade == false {
			validUpgradeTargets = append(validUpgradeTargets, *target)
		}
	}

	if len(validUpgradeTargets) == 0 {
		// no versions to upgrade to
		return nil, nil
	}

	latestUpgradeTarget := validUpgradeTargets[len(validUpgradeTargets)-1]
	return latestUpgradeTarget.EngineVersion, nil
}
