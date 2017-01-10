package rdsbroker

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/frodenas/brokerapi"
	"github.com/mitchellh/mapstructure"
	"github.com/pivotal-golang/lager"

	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/sqlengine"
	"github.com/alphagov/paas-rds-broker/utils"
)

const masterUsernameLength = 16
const masterPasswordLength = 32

const instanceIDLogKey = "instance-id"
const bindingIDLogKey = "binding-id"
const detailsLogKey = "details"
const acceptsIncompleteLogKey = "acceptsIncomplete"
const updateParametersLogKey = "updateParameters"
const servicePlanLogKey = "servicePlan"
const dbInstanceDetailsLogKey = "dbInstanceDetails"

var rdsStatus2State = map[string]string{
	"available":                    brokerapi.LastOperationSucceeded,
	"backing-up":                   brokerapi.LastOperationInProgress,
	"creating":                     brokerapi.LastOperationInProgress,
	"deleting":                     brokerapi.LastOperationInProgress,
	"maintenance":                  brokerapi.LastOperationInProgress,
	"modifying":                    brokerapi.LastOperationInProgress,
	"rebooting":                    brokerapi.LastOperationInProgress,
	"renaming":                     brokerapi.LastOperationInProgress,
	"resetting-master-credentials": brokerapi.LastOperationInProgress,
	"upgrading":                    brokerapi.LastOperationInProgress,
}

type RDSBroker struct {
	dbPrefix                     string
	masterPasswordSeed           string
	allowUserProvisionParameters bool
	allowUserUpdateParameters    bool
	allowUserBindParameters      bool
	catalog                      Catalog
	dbInstance                   awsrds.DBInstance
	sqlProvider                  sqlengine.Provider
	logger                       lager.Logger
	brokerName                   string
}

func New(
	config Config,
	dbInstance awsrds.DBInstance,
	sqlProvider sqlengine.Provider,
	logger lager.Logger,
) *RDSBroker {
	return &RDSBroker{
		dbPrefix:                     config.DBPrefix,
		masterPasswordSeed:           config.MasterPasswordSeed,
		allowUserProvisionParameters: config.AllowUserProvisionParameters,
		allowUserUpdateParameters:    config.AllowUserUpdateParameters,
		allowUserBindParameters:      config.AllowUserBindParameters,
		catalog:                      config.Catalog,
		brokerName:                   config.BrokerName,
		dbInstance:                   dbInstance,
		sqlProvider:                  sqlProvider,
		logger:                       logger.Session("broker"),
	}
}

func (b *RDSBroker) Services() brokerapi.CatalogResponse {
	catalogResponse := brokerapi.CatalogResponse{}

	brokerCatalog, err := json.Marshal(b.catalog)
	if err != nil {
		b.logger.Error("marshal-error", err)
		return catalogResponse
	}

	apiCatalog := brokerapi.Catalog{}
	if err = json.Unmarshal(brokerCatalog, &apiCatalog); err != nil {
		b.logger.Error("unmarshal-error", err)
		return catalogResponse
	}

	catalogResponse.Services = apiCatalog.Services

	return catalogResponse
}

func (b *RDSBroker) Provision(instanceID string, details brokerapi.ProvisionDetails, acceptsIncomplete bool) (brokerapi.ProvisioningResponse, bool, error) {
	b.logger.Debug("provision", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		acceptsIncompleteLogKey: acceptsIncomplete,
	})

	provisioningResponse := brokerapi.ProvisioningResponse{}

	if !acceptsIncomplete {
		return provisioningResponse, false, brokerapi.ErrAsyncRequired
	}

	provisionParameters := ProvisionParameters{}
	if b.allowUserProvisionParameters {
		if err := mapstructure.Decode(details.Parameters, &provisionParameters); err != nil {
			return provisioningResponse, false, err
		}
		if err := provisionParameters.Validate(); err != nil {
			return provisioningResponse, false, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return provisioningResponse, false, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	createDBInstance := b.createDBInstance(instanceID, servicePlan, provisionParameters, details)
	if err := b.dbInstance.Create(b.dbInstanceIdentifier(instanceID), *createDBInstance); err != nil {
		return provisioningResponse, false, err
	}

	return provisioningResponse, true, nil
}

func (b *RDSBroker) Update(instanceID string, details brokerapi.UpdateDetails, acceptsIncomplete bool) (bool, error) {
	b.logger.Debug("update", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		acceptsIncompleteLogKey: acceptsIncomplete,
	})

	if !acceptsIncomplete {
		return false, brokerapi.ErrAsyncRequired
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return false, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if !service.PlanUpdateable {
		return false, brokerapi.ErrInstanceNotUpdateable
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	updateParameters := UpdateParameters{}
	if b.allowUserUpdateParameters {
		if err := mapstructure.Decode(details.Parameters, &updateParameters); err != nil {
			return false, err
		}
		if err := updateParameters.Validate(servicePlan.RDSProperties); err != nil {
			return false, err
		}
		b.logger.Debug("update-parsed-params", lager.Data{updateParametersLogKey: updateParameters})
	}

	modifyDBInstance := b.modifyDBInstance(instanceID, servicePlan, updateParameters, details)
	if err := b.dbInstance.Modify(b.dbInstanceIdentifier(instanceID), *modifyDBInstance, updateParameters.ApplyImmediately); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return false, brokerapi.ErrInstanceDoesNotExist
		}
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) Deprovision(instanceID string, details brokerapi.DeprovisionDetails, acceptsIncomplete bool) (bool, error) {
	b.logger.Debug("deprovision", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		acceptsIncompleteLogKey: acceptsIncomplete,
	})

	if !acceptsIncomplete {
		return false, brokerapi.ErrAsyncRequired
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	skipDBInstanceFinalSnapshot := servicePlan.RDSProperties.SkipFinalSnapshot

	skipFinalSnapshot, err := b.dbInstance.GetTag(b.dbInstanceIdentifier(instanceID), "SkipFinalSnapshot")
	if err != nil {
		return false, err
	}

	if skipFinalSnapshot != "" {
		skipDBInstanceFinalSnapshot, err = strconv.ParseBool(skipFinalSnapshot)
		if err != nil {
			return false, err
		}
	}

	if err := b.dbInstance.Delete(b.dbInstanceIdentifier(instanceID), skipDBInstanceFinalSnapshot); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return false, brokerapi.ErrInstanceDoesNotExist
		}
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) Bind(instanceID, bindingID string, details brokerapi.BindDetails) (brokerapi.BindingResponse, error) {
	b.logger.Debug("bind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	bindingResponse := brokerapi.BindingResponse{}

	bindParameters := BindParameters{}
	if b.allowUserBindParameters {
		if err := mapstructure.Decode(details.Parameters, &bindParameters); err != nil {
			return bindingResponse, err
		}
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return bindingResponse, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if !service.Bindable {
		return bindingResponse, brokerapi.ErrInstanceNotBindable
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return bindingResponse, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	var dbAddress, dbName, masterUsername string
	var dbPort int64
	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return bindingResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return bindingResponse, err
	}

	dbAddress = dbInstanceDetails.Address
	dbPort = dbInstanceDetails.Port
	masterUsername = dbInstanceDetails.MasterUsername
	dbName = b.dbNameFromDetails(instanceID, dbInstanceDetails)

	var replicas []awsrds.DBInstanceDetails
	if len(dbInstanceDetails.ReadReplicaIds) > 0 {
		replicas, err = b.dbInstance.DescribeMany(dbInstanceDetails.ReadReplicaIds)
		if err != nil {
			return bindingResponse, err
		}
	}

	sqlEngine, err := b.sqlProvider.GetSQLEngine(servicePlan.RDSProperties.Engine)
	if err != nil {
		return bindingResponse, err
	}

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.masterPassword(instanceID)); err != nil {
		return bindingResponse, err
	}
	defer sqlEngine.Close()

	dbUsername, dbPassword, err := sqlEngine.CreateUser(bindingID, dbName)
	if err != nil {
		return bindingResponse, err
	}

	replicaURIs := make([]string, len(replicas))
	replicaJDBCURIs := make([]string, len(replicas))
	for idx, replica := range replicas {
		replicaURIs[idx] = sqlEngine.URI(replica.Address, dbPort, dbName, dbUsername, dbPassword)
		replicaJDBCURIs[idx] = sqlEngine.JDBCURI(replica.Address, dbPort, dbName, dbUsername, dbPassword)
	}

	bindingResponse.Credentials = &brokerapi.CredentialsHash{
		Host:            dbAddress,
		Port:            dbPort,
		Name:            dbName,
		Username:        dbUsername,
		Password:        dbPassword,
		URI:             sqlEngine.URI(dbAddress, dbPort, dbName, dbUsername, dbPassword),
		JDBCURI:         sqlEngine.JDBCURI(dbAddress, dbPort, dbName, dbUsername, dbPassword),
		Replicas:        dbInstanceDetails.ReadReplicaIds,
		ReplicaURIs:     replicaURIs,
		ReplicaJDBCURIs: replicaJDBCURIs,
	}

	return bindingResponse, nil
}

func (b *RDSBroker) Unbind(instanceID, bindingID string, details brokerapi.UnbindDetails) error {
	b.logger.Debug("unbind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	var dbAddress, dbName, masterUsername string
	var dbPort int64
	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.ErrInstanceDoesNotExist
		}
		return err
	}

	dbAddress = dbInstanceDetails.Address
	dbPort = dbInstanceDetails.Port
	masterUsername = dbInstanceDetails.MasterUsername
	dbName = b.dbNameFromDetails(instanceID, dbInstanceDetails)

	sqlEngine, err := b.sqlProvider.GetSQLEngine(servicePlan.RDSProperties.Engine)
	if err != nil {
		return err
	}

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.masterPassword(instanceID)); err != nil {
		return err
	}
	defer sqlEngine.Close()

	if err = sqlEngine.DropUser(bindingID); err != nil {
		return err
	}

	return nil
}

func (b *RDSBroker) LastOperation(instanceID string) (brokerapi.LastOperationResponse, error) {
	b.logger.Debug("last-operation", lager.Data{
		instanceIDLogKey: instanceID,
	})

	lastOperationResponse := brokerapi.LastOperationResponse{State: brokerapi.LastOperationFailed}

	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return lastOperationResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return lastOperationResponse, err
	}

	lastOperationResponse.Description = fmt.Sprintf("DB Instance '%s' status is '%s'", b.dbInstanceIdentifier(instanceID), dbInstanceDetails.Status)

	if state, ok := rdsStatus2State[dbInstanceDetails.Status]; ok {
		lastOperationResponse.State = state
	}

	if lastOperationResponse.State == brokerapi.LastOperationSucceeded && dbInstanceDetails.PendingModifications {
		lastOperationResponse.State = brokerapi.LastOperationInProgress
		lastOperationResponse.Description = fmt.Sprintf("DB Instance '%s' has pending modifications", b.dbInstanceIdentifier(instanceID))
	}

	return lastOperationResponse, nil
}

func (b *RDSBroker) CheckAndRotateCredentials() {
	b.logger.Info(fmt.Sprintf("Started checking credentials of RDS instances managed by this broker"))

	dbInstanceDetailsList, err := b.dbInstance.DescribeByTag("Broker Name", b.brokerName)
	if err != nil {
		b.logger.Error("Could not obtain the list of instances", err)
		return
	}

	b.logger.Debug(fmt.Sprintf("Found %v RDS instances managed by the broker", len(dbInstanceDetailsList)))

	for _, dbDetails := range dbInstanceDetailsList {
		b.logger.Debug(fmt.Sprintf("Checking credentials for instance %v", dbDetails.Identifier))
		serviceInstanceID := b.dbInstanceIdentifierToServiceInstanceID(dbDetails.Identifier)
		masterPassword := b.masterPassword(serviceInstanceID)
		dbName := b.dbNameFromDetails(dbDetails.Identifier, *dbDetails)

		sqlEngine, err := b.sqlProvider.GetSQLEngine(dbDetails.Engine)
		if err != nil {
			b.logger.Error(fmt.Sprintf("Could not determine SQL Engine of instance %v", dbDetails.Identifier), err)
			continue
		}

		err = sqlEngine.Open(dbDetails.Address, dbDetails.Port, dbName, dbDetails.MasterUsername, masterPassword)

		if err != nil {
			if err == sqlengine.LoginFailedError {
				b.logger.Info(fmt.Sprintf(
					"Login failed when connecting to DB %v at %v. Will attempt to reset the password.",
					dbName, dbDetails.Address))
				dbDetails.MasterUserPassword = masterPassword
				err = b.dbInstance.Modify(dbDetails.Identifier, *dbDetails, true)
				if err != nil {
					b.logger.Error(fmt.Sprintf("Could not reset the master password of instance %v", dbDetails.Identifier), err)
					continue
				}
			} else {
				b.logger.Error(fmt.Sprintf("Unknown error when connecting to DB %v at %v", dbName, dbDetails.Address), err)
				continue
			}
		}
		defer sqlEngine.Close()
	}
	b.logger.Info(fmt.Sprintf("Instances credentials check has ended"))
}

func (b *RDSBroker) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", strings.Replace(b.dbPrefix, "_", "-", -1), strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSBroker) dbInstanceIdentifierToServiceInstanceID(serviceInstanceID string) string {
	return strings.TrimPrefix(serviceInstanceID, strings.Replace(b.dbPrefix, "_", "-", -1)+"-")
}

func (b *RDSBroker) masterUsername() string {
	return utils.RandomAlphaNum(masterUsernameLength)
}

func (b *RDSBroker) masterPassword(instanceID string) string {
	return utils.GetMD5B64(b.masterPasswordSeed+instanceID, masterPasswordLength)
}

func (b *RDSBroker) dbName(instanceID string) string {
	return fmt.Sprintf("%s_%s", strings.Replace(b.dbPrefix, "-", "_", -1), strings.Replace(instanceID, "-", "_", -1))
}

func (b *RDSBroker) dbNameFromDetails(instanceID string, dbInstanceDetails awsrds.DBInstanceDetails) string {
	var dbName string
	if dbInstanceDetails.DBName != "" {
		dbName = dbInstanceDetails.DBName
	} else {
		dbName = b.dbName(instanceID)
	}
	return dbName
}

func (b *RDSBroker) createDBInstance(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) *awsrds.DBInstanceDetails {
	dbInstanceDetails := b.dbInstanceFromPlan(servicePlan)

	dbInstanceDetails.DBName = b.dbName(instanceID)
	dbInstanceDetails.MasterUsername = b.masterUsername()
	dbInstanceDetails.MasterUserPassword = b.masterPassword(instanceID)

	if provisionParameters.BackupRetentionPeriod > 0 {
		dbInstanceDetails.BackupRetentionPeriod = provisionParameters.BackupRetentionPeriod
	}

	if provisionParameters.CharacterSetName != "" {
		dbInstanceDetails.CharacterSetName = provisionParameters.CharacterSetName
	}

	if provisionParameters.DBName != "" {
		dbInstanceDetails.DBName = provisionParameters.DBName
	}

	if provisionParameters.PreferredBackupWindow != "" {
		dbInstanceDetails.PreferredBackupWindow = provisionParameters.PreferredBackupWindow
	}

	if provisionParameters.PreferredMaintenanceWindow != "" {
		dbInstanceDetails.PreferredMaintenanceWindow = provisionParameters.PreferredMaintenanceWindow
	}

	skipFinalSnapshot := strconv.FormatBool(servicePlan.RDSProperties.SkipFinalSnapshot)
	if provisionParameters.SkipFinalSnapshot != "" {
		skipFinalSnapshot = provisionParameters.SkipFinalSnapshot
	}

	dbInstanceDetails.Tags = b.dbTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshot)

	return dbInstanceDetails
}

func (b *RDSBroker) modifyDBInstance(instanceID string, servicePlan ServicePlan, updateParameters UpdateParameters, details brokerapi.UpdateDetails) *awsrds.DBInstanceDetails {
	dbInstanceDetails := b.dbInstanceFromPlan(servicePlan)

	b.logger.Debug("modifyDBInstance", lager.Data{
		instanceIDLogKey:        instanceID,
		detailsLogKey:           details,
		updateParametersLogKey:  updateParameters,
		servicePlanLogKey:       servicePlan,
		dbInstanceDetailsLogKey: dbInstanceDetails,
	})

	if updateParameters.BackupRetentionPeriod > 0 {
		dbInstanceDetails.BackupRetentionPeriod = updateParameters.BackupRetentionPeriod
	}

	if updateParameters.PreferredBackupWindow != "" {
		dbInstanceDetails.PreferredBackupWindow = updateParameters.PreferredBackupWindow
	}

	if updateParameters.PreferredMaintenanceWindow != "" {
		dbInstanceDetails.PreferredMaintenanceWindow = updateParameters.PreferredMaintenanceWindow
	}

	skipFinalSnapshot := strconv.FormatBool(servicePlan.RDSProperties.SkipFinalSnapshot)
	if updateParameters.SkipFinalSnapshot != "" {
		skipFinalSnapshot = updateParameters.SkipFinalSnapshot
	}

	dbInstanceDetails.Tags = b.dbTags("Updated", details.ServiceID, details.PlanID, "", "", skipFinalSnapshot)

	return dbInstanceDetails
}

func (b *RDSBroker) dbInstanceFromPlan(servicePlan ServicePlan) *awsrds.DBInstanceDetails {
	dbInstanceDetails := &awsrds.DBInstanceDetails{
		DBInstanceClass: servicePlan.RDSProperties.DBInstanceClass,
		Engine:          servicePlan.RDSProperties.Engine,
	}

	dbInstanceDetails.AutoMinorVersionUpgrade = servicePlan.RDSProperties.AutoMinorVersionUpgrade

	if servicePlan.RDSProperties.AvailabilityZone != "" {
		dbInstanceDetails.AvailabilityZone = servicePlan.RDSProperties.AvailabilityZone
	}

	dbInstanceDetails.CopyTagsToSnapshot = servicePlan.RDSProperties.CopyTagsToSnapshot

	if servicePlan.RDSProperties.DBParameterGroupName != "" {
		dbInstanceDetails.DBParameterGroupName = servicePlan.RDSProperties.DBParameterGroupName
	}

	if servicePlan.RDSProperties.DBSubnetGroupName != "" {
		dbInstanceDetails.DBSubnetGroupName = servicePlan.RDSProperties.DBSubnetGroupName
	}

	if servicePlan.RDSProperties.EngineVersion != "" {
		dbInstanceDetails.EngineVersion = servicePlan.RDSProperties.EngineVersion
	}

	if servicePlan.RDSProperties.OptionGroupName != "" {
		dbInstanceDetails.OptionGroupName = servicePlan.RDSProperties.OptionGroupName
	}

	if servicePlan.RDSProperties.PreferredMaintenanceWindow != "" {
		dbInstanceDetails.PreferredMaintenanceWindow = servicePlan.RDSProperties.PreferredMaintenanceWindow
	}

	dbInstanceDetails.PubliclyAccessible = servicePlan.RDSProperties.PubliclyAccessible

	dbInstanceDetails.BackupRetentionPeriod = servicePlan.RDSProperties.BackupRetentionPeriod

	if servicePlan.RDSProperties.AllocatedStorage > 0 {
		dbInstanceDetails.AllocatedStorage = servicePlan.RDSProperties.AllocatedStorage
	}

	if servicePlan.RDSProperties.CharacterSetName != "" {
		dbInstanceDetails.CharacterSetName = servicePlan.RDSProperties.CharacterSetName
	}

	if len(servicePlan.RDSProperties.DBSecurityGroups) > 0 {
		dbInstanceDetails.DBSecurityGroups = servicePlan.RDSProperties.DBSecurityGroups
	}

	if servicePlan.RDSProperties.Iops > 0 {
		dbInstanceDetails.Iops = servicePlan.RDSProperties.Iops
	}

	if servicePlan.RDSProperties.KmsKeyID != "" {
		dbInstanceDetails.KmsKeyID = servicePlan.RDSProperties.KmsKeyID
	}

	if servicePlan.RDSProperties.LicenseModel != "" {
		dbInstanceDetails.LicenseModel = servicePlan.RDSProperties.LicenseModel
	}

	dbInstanceDetails.MultiAZ = servicePlan.RDSProperties.MultiAZ

	if servicePlan.RDSProperties.Port > 0 {
		dbInstanceDetails.Port = servicePlan.RDSProperties.Port
	}

	if servicePlan.RDSProperties.PreferredBackupWindow != "" {
		dbInstanceDetails.PreferredBackupWindow = servicePlan.RDSProperties.PreferredBackupWindow
	}

	dbInstanceDetails.StorageEncrypted = servicePlan.RDSProperties.StorageEncrypted

	if servicePlan.RDSProperties.StorageType != "" {
		dbInstanceDetails.StorageType = servicePlan.RDSProperties.StorageType
	}

	if len(servicePlan.RDSProperties.VpcSecurityGroupIds) > 0 {
		dbInstanceDetails.VpcSecurityGroupIds = servicePlan.RDSProperties.VpcSecurityGroupIds
	}

	return dbInstanceDetails
}

func (b *RDSBroker) dbTags(action, serviceID, planID, organizationID, spaceID, skipFinalSnapshot string) map[string]string {
	tags := make(map[string]string)

	tags["Owner"] = "Cloud Foundry"

	tags["Broker Name"] = b.brokerName

	tags[action+" by"] = "AWS RDS Service Broker"

	tags[action+" at"] = time.Now().Format(time.RFC822Z)

	if serviceID != "" {
		tags["Service ID"] = serviceID
	}

	if planID != "" {
		tags["Plan ID"] = planID
	}

	if organizationID != "" {
		tags["Organization ID"] = organizationID
	}

	if spaceID != "" {
		tags["Space ID"] = spaceID
	}

	if skipFinalSnapshot != "" {
		tags["SkipFinalSnapshot"] = skipFinalSnapshot
	}

	return tags
}
