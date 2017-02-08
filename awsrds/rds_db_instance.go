package awsrds

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pivotal-golang/lager"
)

type RDSDBInstance struct {
	region    string
	partition string
	rdssvc    *rds.RDS
	stssvc    *sts.STS
	logger    lager.Logger
}

func NewRDSDBInstance(
	region string,
	partition string,
	rdssvc *rds.RDS,
	stssvc *sts.STS,
	logger lager.Logger,
) *RDSDBInstance {
	return &RDSDBInstance{
		region:    region,
		partition: partition,
		rdssvc:    rdssvc,
		stssvc:    stssvc,
		logger:    logger.Session("db-instance"),
	}
}

func (r *RDSDBInstance) Describe(ID string) (DBInstanceDetails, error) {
	dbInstanceDetails := DBInstanceDetails{}

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(ID),
	}

	r.logger.Debug("describe-db-instances", lager.Data{"input": describeDBInstancesInput})

	dbInstances, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return dbInstanceDetails, ErrDBInstanceDoesNotExist
				}
			}
			return dbInstanceDetails, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return dbInstanceDetails, err
	}

	for _, dbInstance := range dbInstances.DBInstances {
		if aws.StringValue(dbInstance.DBInstanceIdentifier) == ID {
			r.logger.Debug("describe-db-instances", lager.Data{"db-instance": dbInstance})
			return r.buildDBInstance(dbInstance), nil
		}
	}

	return dbInstanceDetails, ErrDBInstanceDoesNotExist
}

func (r *RDSDBInstance) DescribeByTag(tagKey, tagValue string) ([]*DBInstanceDetails, error) {
	dbInstanceDetails := []*DBInstanceDetails{}

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{}

	dbInstances, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)

	if err != nil {
		return dbInstanceDetails, err
	}
	for _, dbInstance := range dbInstances.DBInstances {
		dbArn, err := r.dbInstanceARN(*dbInstance.DBInstanceIdentifier)
		if err != nil {
			return dbInstanceDetails, err
		}
		listTagsForResourceInput := &rds.ListTagsForResourceInput{
			ResourceName: aws.String(dbArn),
		}
		listTagsForResourceOutput, err := r.rdssvc.ListTagsForResource(listTagsForResourceInput)
		if err != nil {
			return dbInstanceDetails, err
		}
		for _, t := range listTagsForResourceOutput.TagList {
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
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return dbSnapshotDetails, ErrDBInstanceDoesNotExist
				}
			}
			return dbSnapshotDetails, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return dbSnapshotDetails, err
	}

	for _, dbSnapshot := range describeDBSnapshotsOutput.DBSnapshots {
		s := r.buildDBSnapshot(dbSnapshot)
		s.Arn, err = r.dbSnapshotARN(s.Identifier)
		if err != nil {
			r.logger.Error("aws-rds-error", err)
			if awsErr, ok := err.(awserr.Error); ok {
				return dbSnapshotDetails, errors.New(awsErr.Code() + ": " + awsErr.Message())
			}
			return dbSnapshotDetails, err
		}
		t, err := ListTagsForResource(s.Arn, r.rdssvc, r.logger)
		if err != nil {
			r.logger.Error("aws-rds-error", err)
			if awsErr, ok := err.(awserr.Error); ok {
				return dbSnapshotDetails, errors.New(awsErr.Code() + ": " + awsErr.Message())
			}
			return dbSnapshotDetails, err
		}
		s.Tags = RDSTagsValues(t)
		dbSnapshotDetails = append(dbSnapshotDetails, &s)
	}

	sort.Sort(ByCreateTime(dbSnapshotDetails))
	return dbSnapshotDetails, nil
}

func (r *RDSDBInstance) GetTag(ID, tagKey string) (string, error) {

	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(ID),
	}

	r.logger.Debug("get-tag", lager.Data{"input": describeDBInstancesInput})

	myInstance, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return "", ErrDBInstanceDoesNotExist
				}
			}
			return "", errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return "", err
	}

	dbArn, err := r.dbInstanceARN(*myInstance.DBInstances[0].DBInstanceIdentifier)
	if err != nil {
		return "", err
	}

	listTagsForResourceInput := &rds.ListTagsForResourceInput{
		ResourceName: aws.String(dbArn),
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
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	r.logger.Debug("create-db-instance", lager.Data{"output": createDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) Restore(ID, snapshotIdentifier string, dbInstanceDetails DBInstanceDetails) error {
	restoreDBInstanceInput := r.buildRestoreDBInstanceInput(ID, snapshotIdentifier, dbInstanceDetails)
	r.logger.Debug("restore-db-instance", lager.Data{"input": &restoreDBInstanceInput})

	restoreDBInstanceOutput, err := r.rdssvc.RestoreDBInstanceFromDBSnapshot(restoreDBInstanceInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	r.logger.Debug("restore-db-instance", lager.Data{"output": restoreDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) Modify(ID string, dbInstanceDetails DBInstanceDetails, applyImmediately bool) error {
	oldDBInstanceDetails, err := r.Describe(ID)
	if err != nil {
		return err
	}

	if dbInstanceDetails.Engine != "" && strings.ToLower(oldDBInstanceDetails.Engine) != strings.ToLower(dbInstanceDetails.Engine) {
		return fmt.Errorf("Migrating the RDS DB Instance engine from '%s' to '%s' is not supported", oldDBInstanceDetails.Engine, dbInstanceDetails.Engine)
	}

	modifyDBInstanceInput := r.buildModifyDBInstanceInput(ID, dbInstanceDetails, oldDBInstanceDetails, applyImmediately)

	sanitizedDBInstanceInput := *modifyDBInstanceInput
	sanitizedDBInstanceInput.MasterUserPassword = aws.String("REDACTED")
	r.logger.Debug("modify-db-instance", lager.Data{"input": &sanitizedDBInstanceInput})

	modifyDBInstanceOutput, err := r.rdssvc.ModifyDBInstance(modifyDBInstanceInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return ErrDBInstanceDoesNotExist
				}
			}
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	r.logger.Debug("modify-db-instance", lager.Data{"output": modifyDBInstanceOutput})

	if len(dbInstanceDetails.Tags) > 0 {
		dbInstanceARN, err := r.dbInstanceARN(ID)
		if err != nil {
			return nil
		}

		tags := BuilRDSTags(dbInstanceDetails.Tags)
		AddTagsToResource(dbInstanceARN, tags, r.rdssvc, r.logger)
	}

	return nil
}

func (r *RDSDBInstance) Delete(ID string, skipFinalSnapshot bool) error {
	deleteDBInstanceInput := r.buildDeleteDBInstanceInput(ID, skipFinalSnapshot)
	r.logger.Debug("delete-db-instance", lager.Data{"input": deleteDBInstanceInput})

	deleteDBInstanceOutput, err := r.rdssvc.DeleteDBInstance(deleteDBInstanceInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return ErrDBInstanceDoesNotExist
				}
			}
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	r.logger.Debug("delete-db-instance", lager.Data{"output": deleteDBInstanceOutput})

	return nil
}

func (r *RDSDBInstance) buildDBInstance(dbInstance *rds.DBInstance) DBInstanceDetails {
	dbInstanceDetails := DBInstanceDetails{
		Identifier:       aws.StringValue(dbInstance.DBInstanceIdentifier),
		Status:           aws.StringValue(dbInstance.DBInstanceStatus),
		Engine:           aws.StringValue(dbInstance.Engine),
		EngineVersion:    aws.StringValue(dbInstance.EngineVersion),
		DBName:           aws.StringValue(dbInstance.DBName),
		MasterUsername:   aws.StringValue(dbInstance.MasterUsername),
		AllocatedStorage: aws.Int64Value(dbInstance.AllocatedStorage),
	}

	if dbInstance.Endpoint != nil {
		dbInstanceDetails.Address = aws.StringValue(dbInstance.Endpoint.Address)
		dbInstanceDetails.Port = aws.Int64Value(dbInstance.Endpoint.Port)
	}

	if dbInstance.PendingModifiedValues != nil {
		emptyPendingModifiedValues := &rds.PendingModifiedValues{}
		if *dbInstance.PendingModifiedValues != *emptyPendingModifiedValues {
			dbInstanceDetails.PendingModifications = true
		}
	}

	return dbInstanceDetails
}

func (r *RDSDBInstance) buildDBSnapshot(dbSnapshot *rds.DBSnapshot) DBSnapshotDetails {
	dbSnapshotDetails := DBSnapshotDetails{
		Identifier:         aws.StringValue(dbSnapshot.DBSnapshotIdentifier),
		InstanceIdentifier: aws.StringValue(dbSnapshot.DBInstanceIdentifier),
		CreateTime:         aws.TimeValue(dbSnapshot.InstanceCreateTime),
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

func (r *RDSDBInstance) buildModifyDBInstanceInput(ID string, dbInstanceDetails DBInstanceDetails, oldDBInstanceDetails DBInstanceDetails, applyImmediately bool) *rds.ModifyDBInstanceInput {
	modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String(ID),
		ApplyImmediately:     aws.Bool(applyImmediately),
	}

	if dbInstanceDetails.AllocatedStorage > 0 {
		if dbInstanceDetails.AllocatedStorage < oldDBInstanceDetails.AllocatedStorage {
			modifyDBInstanceInput.AllocatedStorage = aws.Int64(oldDBInstanceDetails.AllocatedStorage)
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

	if dbInstanceDetails.EngineVersion != "" && dbInstanceDetails.EngineVersion != oldDBInstanceDetails.EngineVersion {
		modifyDBInstanceInput.EngineVersion = aws.String(dbInstanceDetails.EngineVersion)
		modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(r.allowMajorVersionUpgrade(dbInstanceDetails.EngineVersion, oldDBInstanceDetails.EngineVersion))
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

//FIXME: when the github.com/aws/aws-sdk-go/service/rds dependency is
// updated we can extract the ARN directly from the rds.DBInstance struct
// https://godoc.org/github.com/aws/aws-sdk-go/service/rds#DBInstance
func (r *RDSDBInstance) dbInstanceARN(ID string) (string, error) {
	userAccount, err := UserAccount(r.stssvc)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("arn:%s:rds:%s:%s:db:%s", r.partition, r.region, userAccount, ID), nil
}

//FIXME: when the github.com/aws/aws-sdk-go/service/rds dependency is
// updated we can extract the ARN directly from the rds.DBSnapshot struct
// https://godoc.org/github.com/aws/aws-sdk-go/service/rds#DBSnapshot
func (r *RDSDBInstance) dbSnapshotARN(ID string) (string, error) {
	userAccount, err := UserAccount(r.stssvc)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("arn:%s:rds:%s:%s:snapshot:%s", r.partition, r.region, userAccount, ID), nil
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
