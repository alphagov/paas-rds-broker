package awsrds

import (
	"errors"
	"fmt"
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

func (r *RDSDBInstance) DescribeMany(IDs []string) ([]DBInstanceDetails, error) {
	instances := []DBInstanceDetails{}

	values := make([]*string, len(IDs))
	for idx, value := range IDs {
		values[idx] = aws.String(value)
	}
	describeDBInstancesInput := &rds.DescribeDBInstancesInput{
		Filters: []*rds.Filter{{
			Name:   aws.String("db-instance-id"),
			Values: values,
		}},
	}

	r.logger.Debug("describe-db-instances", lager.Data{"input": describeDBInstancesInput})

	dbInstances, err := r.rdssvc.DescribeDBInstances(describeDBInstancesInput)
	if err != nil {
		r.logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return instances, ErrDBInstanceDoesNotExist
				}
			}
			return instances, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return instances, err
	}

	for _, dbInstance := range dbInstances.DBInstances {
		instances = append(instances, r.buildDBInstance(dbInstance))
	}

	return instances, nil
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

func (r *RDSDBInstance) listReplicas(ID string) ([]string, error) {
	details, err := r.Describe(ID)
	if err != nil {
		return []string{}, err
	}
	return details.ReadReplicaIds, nil
}

func (r *RDSDBInstance) scaleReplicas(ID string, dbInstanceDetails DBInstanceDetails) error {
	replicas, err := r.listReplicas(ID)
	if err != nil {
		return err
	}

	count := dbInstanceDetails.ReadReplicaCount
	if len(replicas) > count {
		for i := count; i < len(replicas); i++ {
			err = r.deleteInstance(replicas[i], true)
			if err != nil {
				return err
			}
		}
	} else if len(replicas) < count {
		for i := len(replicas); i < count; i++ {
			instanceID := fmt.Sprintf("%s-rr%d", ID, i)
			createReplicaInput := r.buildCreateDBInstanceReadReplicaInput(instanceID, ID, dbInstanceDetails)
			r.logger.Debug("create-replica", lager.Data{"input": createReplicaInput})

			createReplicaOutput, err := r.rdssvc.CreateDBInstanceReadReplica(createReplicaInput)
			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok {
					return errors.New(awsErr.Code() + ": " + awsErr.Message())
				}
				return err
			}
			r.logger.Debug("create-replica", lager.Data{"output": createReplicaOutput})
		}
	}

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

	if dbInstanceDetails.ReadReplicaCount > 0 && oldDBInstanceDetails.BackupRetentionPeriod == 0 {
		return ErrCannotCreateReplicaWithoutBackups
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

	if err := r.scaleReplicas(ID, dbInstanceDetails); err != nil {
		return err
	}

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
	replicas, err := r.listReplicas(ID)
	if err != nil {
		return err
	}

	for _, replica := range replicas {
		if err := r.deleteInstance(replica, true); err != nil {
			return err
		}
	}

	return r.deleteInstance(ID, skipFinalSnapshot)
}

func (r *RDSDBInstance) deleteInstance(ID string, skipFinalSnapshot bool) error {
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
		SourceIdentifier: aws.StringValue(dbInstance.ReadReplicaSourceDBInstanceIdentifier),
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

	for _, id := range dbInstance.ReadReplicaDBInstanceIdentifiers {
		dbInstanceDetails.ReadReplicaIds = append(dbInstanceDetails.ReadReplicaIds, aws.StringValue(id))
	}

	return dbInstanceDetails
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

func (r *RDSDBInstance) buildCreateDBInstanceReadReplicaInput(ID, sourceID string, dbInstanceDetails DBInstanceDetails) *rds.CreateDBInstanceReadReplicaInput {
	createDBInstanceInput := &rds.CreateDBInstanceReadReplicaInput{
		DBInstanceIdentifier:       aws.String(ID),
		SourceDBInstanceIdentifier: aws.String(sourceID),
	}

	createDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(dbInstanceDetails.AutoMinorVersionUpgrade)

	if dbInstanceDetails.AvailabilityZone != "" {
		createDBInstanceInput.AvailabilityZone = aws.String(dbInstanceDetails.AvailabilityZone)
	}

	createDBInstanceInput.CopyTagsToSnapshot = aws.Bool(dbInstanceDetails.CopyTagsToSnapshot)

	if dbInstanceDetails.DBInstanceClass != "" {
		createDBInstanceInput.DBInstanceClass = aws.String(dbInstanceDetails.DBInstanceClass)
	}

	if dbInstanceDetails.OptionGroupName != "" {
		createDBInstanceInput.OptionGroupName = aws.String(dbInstanceDetails.OptionGroupName)
	}

	if dbInstanceDetails.Port > 0 {
		createDBInstanceInput.Port = aws.Int64(dbInstanceDetails.Port)
	}

	createDBInstanceInput.PubliclyAccessible = aws.Bool(dbInstanceDetails.PubliclyAccessible)

	if dbInstanceDetails.StorageType != "" {
		createDBInstanceInput.StorageType = aws.String(dbInstanceDetails.StorageType)
	}

	if dbInstanceDetails.Iops > 0 {
		createDBInstanceInput.Iops = aws.Int64(dbInstanceDetails.Iops)
	}

	if len(dbInstanceDetails.Tags) > 0 {
		createDBInstanceInput.Tags = BuilRDSTags(dbInstanceDetails.Tags)
	}

	return createDBInstanceInput
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

func (r *RDSDBInstance) dbInstanceARN(ID string) (string, error) {
	userAccount, err := UserAccount(r.stssvc)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("arn:%s:rds:%s:%s:db:%s", r.partition, r.region, userAccount, ID), nil
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
