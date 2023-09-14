package awsrds

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/lager/v3"
	"github.com/Masterminds/semver"
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
	region           string
	partition        string
	rdssvc           *rds.RDS
	cachedTags       map[string]tagCacheEntry
	cachedTagsLock   sync.RWMutex
	logger           lager.Logger
	timeNowFunc      func() time.Time
	tagCacheDuration time.Duration
}

type tagCacheEntry struct {
	tags        []*rds.Tag
	requestTime time.Time
}

func (e *tagCacheEntry) HasExpired(now time.Time, duration time.Duration) bool {
	return now.After(e.requestTime.Add(duration))
}

func NewRDSDBInstance(
	region string,
	partition string,
	rdssvc *rds.RDS,
	logger lager.Logger,
	tagCacheDuration time.Duration,
	timeNowFunc func() time.Time,
) *RDSDBInstance {
	if timeNowFunc == nil {
		timeNowFunc = func() time.Time {
			return time.Now()
		}
	}

	return &RDSDBInstance{
		region:           region,
		partition:        partition,
		rdssvc:           rdssvc,
		cachedTags:       map[string]tagCacheEntry{},
		logger:           logger.Session("db-instance"),
		tagCacheDuration: tagCacheDuration,
		timeNowFunc:      timeNowFunc,
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
	useCached := false
	for _, o := range opts {
		if o == DescribeUseCachedOption {
			useCached = true
		}
	}

	r.logger.Debug("get-resource-tags", lager.Data{"arn": resourceArn, "use-cached": useCached})

	t, err := r.cachedListTagsForResource(resourceArn, useCached)
	if err != nil {
		return nil, HandleAWSError(err, r.logger)
	}
	return t, nil
}

func (r *RDSDBInstance) DescribeByTag(tagKey, tagValue string, opts ...DescribeOption) ([]*rds.DBInstance, error) {
	alllDbInstances := []*rds.DBInstance{}

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{}

	useCached := false
	for _, o := range opts {
		if o == DescribeUseCachedOption {
			useCached = true
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
		tags, err := r.cachedListTagsForResource(
			aws.StringValue(dbInstance.DBInstanceArn),
			useCached,
		)
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

	deleteBefore := r.timeNowFunc().Add(-1 * time.Duration(keepForDays) * 24 * time.Hour)

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
		tags, err := r.cachedListTagsForResource(
			aws.StringValue(snapshot.DBSnapshotArn),
			false,
		)
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

	tags, err := r.cachedListTagsForResource(
		aws.StringValue(myInstance.DBInstances[0].DBInstanceArn),
		false,
	)
	if err != nil {
		return "", err
	}

	for _, t := range tags {
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
		if newAllocatedSpace == oldAllocatedSpace {
			updatedModifyDBInstanceInput.AllocatedStorage = nil
			r.logger.Info("modify-db-instance.storage-unchanged", lager.Data{"input": &sanitizedDBInstanceInput})
		} else if newAllocatedSpace < oldAllocatedSpace {
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

	newParameterGroup := aws.StringValue(modifyDBInstanceInput.DBParameterGroupName)
	oldParameterGroup := ""
	if len(oldDbInstance.DBParameterGroups) == 1 {
		oldParameterGroup = aws.StringValue(oldDbInstance.DBParameterGroups[0].DBParameterGroupName)
	}
	if newParameterGroup == oldParameterGroup {
		updatedModifyDBInstanceInput.DBParameterGroupName = nil
		r.logger.Info("modify-db-instance.prevented-update-same-parametergroup", lager.Data{"input": &sanitizedDBInstanceInput})
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

func (r *RDSDBInstance) cachedListTagsForResource(arn string, useCached bool) ([]*rds.Tag, error) {
	if useCached {
		r.cachedTagsLock.RLock()
		entry, ok := r.cachedTags[arn]
		r.cachedTagsLock.RUnlock()
		if ok && !entry.HasExpired(r.timeNowFunc(), r.tagCacheDuration) {
			return entry.tags, nil
		}
	}

	tags, err := ListTagsForResource(arn, r.rdssvc, r.logger)
	if err == nil {
		entry := tagCacheEntry{
			tags:        tags,
			requestTime: r.timeNowFunc(),
		}
		r.cachedTagsLock.Lock()
		r.cachedTags[arn] = entry
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

// GetFullValidTargetVersion finds the full version specifier for the newest release of the target version.
// engine is the name of the database engine in AWS RDS (e.g. postgres).
// currentVersion is current, exact version of a database engine
// targetVersionMoniker is the name of a version of the database engine. It does not include the patch/minor level information.
//
//	E.g. For postgres, 9.5 is the moniker for 9.5.x, and 10 is the moniker for 10.x
//
// if no upgrades are available for the major version and the targetVersionMoniker is the same major version as the current version,
// an empty string is returned. This should be interpreted as a signal to omit an engine version upgrade attempt.
func (r *RDSDBInstance) GetFullValidTargetVersion(engine string, currentVersion string, targetVersionMoniker string) (string, error) {
	logSess := r.logger.Session("get-full-valid-target-version",
		lager.Data{"engine": engine, "version": currentVersion, "targetVersionMoniker": targetVersionMoniker})

	currentSemVersion, err := semver.NewVersion(currentVersion)
	if err != nil {
		logSess.Error("parse-current-version-as-semver", err)
		return "", err
	}

	targetMonikerSemVer, err := semver.NewVersion(targetVersionMoniker)
	if err != nil {
		logSess.Error("parse-target-version-moniker-as-semver", err)
		return "", err
	}

	logSess.Info("describe-db-engine-versions")
	engineVersionsOut, err := r.rdssvc.DescribeDBEngineVersions(&rds.DescribeDBEngineVersionsInput{
		Engine:        aws.String(engine),
		EngineVersion: aws.String(currentVersion),
	})

	if err != nil {
		logSess.Error("describe-db-engine-versions", err)
		return "", err
	}

	if len(engineVersionsOut.DBEngineVersions) == 0 {
		err = fmt.Errorf("describe-db-engines did not describe a version engine matching the engine and current version")
		logSess.Error("no-matching-engine-version", err)
		return "", err
	}

	if len(engineVersionsOut.DBEngineVersions) > 1 {
		err = fmt.Errorf("given version '%s' was too broad. Current version must specify an exact version", currentVersion)
		logSess.Error("ambiguous-version", err)
		return "", err
	}

	var targetVersions []string
	for _, target := range engineVersionsOut.DBEngineVersions[0].ValidUpgradeTarget {
		targetVersions = append(targetVersions, *target.EngineVersion)
	}

	semVersions, err := parseSemanticVersions(targetVersions)
	targettableVersions := filterTargetVersion(semVersions, engine, *targetMonikerSemVer)
	if len(targettableVersions) == 0 {
		if currentSemVersion.Major() == targetMonikerSemVer.Major() {
			logSess.Info("no-new-version-but-same-major-noop", lager.Data{
				"target-version-moniker": targetVersionMoniker,
				"current-version":        currentVersion,
			})
			return "", nil
		}
		err := fmt.Errorf("no valid targets for target major version '%s'", targetVersionMoniker)
		logSess.Error("no-upgrade-targets-for-target-version", err)
		return "", err
	}

	newestTargettableVersion := findNewestTargettableVersion(targettableVersions)

	formattedVersion := formatEngineVersion(newestTargettableVersion, engine)
	logSess.Info("selected-version", lager.Data{"version": formattedVersion})

	return formattedVersion, nil
}

func parseSemanticVersions(versions []string) (semver.Collection, error) {
	collection := semver.Collection{}
	for _, version := range versions {
		sv, err := semver.NewVersion(version)
		if err != nil {
			return nil, err
		}

		collection = append(collection, sv)
	}

	return collection, nil
}

func filterTargetVersion(versions semver.Collection, engine string, targetSemVer semver.Version) semver.Collection {
	// Filter target version needs to be a bit smart because Postgres changed its versioning strategy as of
	// version 10. It used to be that a version was `x.y.PATCH`. It's now `x.PATCH`.
	// Meanwhile, mysql has maintained `x.y.PATCH`.
	var comparsionStrategy func(version *semver.Version, targetVersionMoniker semver.Version) bool
	var semverMajorVersionCompare = func(version *semver.Version, targetVersionMoniker semver.Version) bool {
		return version.Major() == targetSemVer.Major()
	}
	var semverMajorMinorVersionCompare = func(version *semver.Version, targetVersionMoniker semver.Version) bool {
		return (version.Major() == targetSemVer.Major()) && (version.Minor() == targetSemVer.Minor())
	}

	switch engine {
	case "postgres":
		if targetSemVer.Major() > 9 {
			comparsionStrategy = semverMajorVersionCompare
		} else {
			comparsionStrategy = semverMajorMinorVersionCompare
		}
	case "mysql":
		comparsionStrategy = semverMajorMinorVersionCompare
	default:
		comparsionStrategy = semverMajorVersionCompare
	}

	collection := semver.Collection{}
	for _, v := range versions {
		if v != nil && comparsionStrategy(v, targetSemVer) {
			collection = append(collection, v)
		}
	}

	return collection
}

func findNewestTargettableVersion(versions semver.Collection) semver.Version {
	sort.Sort(versions)
	return *versions[versions.Len()-1]
}

// formatEngineVersion formats a given semantic version in accordance
// with the versioning rules of the engine.
func formatEngineVersion(version semver.Version, engine string) string {
	switch engine {
	default:
	case "mysql":
		// Mysql uses MAJOR.MINOR.PATCH format
		return fmt.Sprintf("%d.%d.%d", version.Major(), version.Minor(), version.Patch())
	case "postgres":
		// Postgres 10 and above uses the MAJOR.MINOR format
		if version.Major() >= 10 {
			return fmt.Sprintf("%d.%d", version.Major(), version.Minor())
		} else {
			// And postgres 9 and below uses MAJOR.MINOR.PATCH
			return fmt.Sprintf("%d.%d.%d", version.Major(), version.Minor(), version.Patch())
		}
	}

	return ""
}
