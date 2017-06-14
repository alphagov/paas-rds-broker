package rdsbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"

	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/sqlengine"
	"github.com/alphagov/paas-rds-broker/utils"
)

const masterUsernameLength = 16
const masterPasswordLength = 32

const instanceIDLogKey = "instance-id"
const bindingIDLogKey = "binding-id"
const detailsLogKey = "details"
const asyncAllowedLogKey = "acceptsIncomplete"
const updateParametersLogKey = "updateParameters"
const servicePlanLogKey = "servicePlan"
const dbInstanceDetailsLogKey = "dbInstanceDetails"
const lastOperationResponseLogKey = "lastOperationResponse"

var (
	ErrEncryptionNotUpdateable = errors.New("instance can not be updated to a plan with different encryption settings")
)

var rdsStatus2State = map[string]brokerapi.LastOperationState{
	"available":                    brokerapi.Succeeded,
	"backing-up":                   brokerapi.InProgress,
	"creating":                     brokerapi.InProgress,
	"deleting":                     brokerapi.InProgress,
	"maintenance":                  brokerapi.InProgress,
	"modifying":                    brokerapi.InProgress,
	"rebooting":                    brokerapi.InProgress,
	"renaming":                     brokerapi.InProgress,
	"resetting-master-credentials": brokerapi.InProgress,
	"upgrading":                    brokerapi.InProgress,
}

const StateUpdateSettings = "PendingUpdateSettings"
const StateReboot = "PendingReboot"
const StateResetUserPassword = "PendingResetUserPassword"

var restoreStateSequence = []string{StateUpdateSettings, StateReboot, StateResetUserPassword}

const TagServiceID = "Service ID"
const TagPlanID = "Plan ID"
const TagOrganizationID = "Organization ID"
const TagSpaceID = "Space ID"
const TagSkipFinalSnapshot = "SkipFinalSnapshot"
const TagRestoredFromSnapshot = "Restored From Snapshot"

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

type Credentials struct {
	Host     string `json:"host"`
	Port     int64  `json:"port"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Password string `json:"password"`
	URI      string `json:"uri"`
	JDBCURI  string `json:"jdbcuri"`
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

func (b *RDSBroker) Services(ctx context.Context) []brokerapi.Service {
	brokerCatalog, err := json.Marshal(b.catalog)
	if err != nil {
		b.logger.Error("marshal-error", err)
		return []brokerapi.Service{}
	}

	apiCatalog := CatalogExternal{}
	if err = json.Unmarshal(brokerCatalog, &apiCatalog); err != nil {
		b.logger.Error("unmarshal-error", err)
		return []brokerapi.Service{}
	}

	for i := range apiCatalog.Services {
		apiCatalog.Services[i].Bindable = true
	}

	return apiCatalog.Services
}

func (b *RDSBroker) Provision(
	ctx context.Context,
	instanceID string,
	details brokerapi.ProvisionDetails,
	asyncAllowed bool,
) (brokerapi.ProvisionedServiceSpec, error) {
	b.logger.Debug("provision", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	if !asyncAllowed {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	provisionParameters := ProvisionParameters{}
	if b.allowUserProvisionParameters && len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &provisionParameters); err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
		if err := provisionParameters.Validate(); err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	if provisionParameters.RestoreFromLatestSnapshotOf == nil {
		createDBInstance := b.createDBInstance(instanceID, servicePlan, provisionParameters, details)
		if err := b.dbInstance.Create(b.dbInstanceIdentifier(instanceID), *createDBInstance); err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
	} else {
		if *provisionParameters.RestoreFromLatestSnapshotOf == "" {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Invalid guid: '%s'", *provisionParameters.RestoreFromLatestSnapshotOf)
		}
		if servicePlan.RDSProperties.Engine != "postgres" {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Restore from snapshot not supported for engine '%s'", servicePlan.RDSProperties.Engine)
		}
		restoreFromDBInstanceID := b.dbInstanceIdentifier(*provisionParameters.RestoreFromLatestSnapshotOf)
		snapshots, err := b.dbInstance.DescribeSnapshots(restoreFromDBInstanceID)
		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
		if len(snapshots) == 0 {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("No snapshots found for guid '%s'", *provisionParameters.RestoreFromLatestSnapshotOf)
		}
		snapshot := snapshots[0]
		if snapshot.Tags[TagSpaceID] != details.SpaceGUID || snapshot.Tags[TagOrganizationID] != details.OrganizationGUID {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("The service instance you are getting a snapshot from is not in the same org or space")
		}
		if snapshot.Tags[TagPlanID] != details.PlanID {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("You must use the same plan as the service instance you are getting a snapshot from")
		}
		snapshotIdentifier := snapshot.Identifier
		restoreDBInstance := b.restoreDBInstance(instanceID, snapshotIdentifier, servicePlan, provisionParameters, details)
		if err := b.dbInstance.Restore(b.dbInstanceIdentifier(instanceID), snapshotIdentifier, *restoreDBInstance); err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
	}

	return brokerapi.ProvisionedServiceSpec{IsAsync: true}, nil
}

func (b *RDSBroker) Update(
	ctx context.Context,
	instanceID string,
	details brokerapi.UpdateDetails,
	asyncAllowed bool,
) (brokerapi.UpdateServiceSpec, error) {
	b.logger.Debug("update", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	if !asyncAllowed {
		return brokerapi.UpdateServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	updateParameters := UpdateParameters{}
	if b.allowUserUpdateParameters && len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &updateParameters); err != nil {
			return brokerapi.UpdateServiceSpec{}, err
		}
		if err := updateParameters.Validate(); err != nil {
			return brokerapi.UpdateServiceSpec{}, err
		}
		b.logger.Debug("update-parsed-params", lager.Data{updateParametersLogKey: updateParameters})
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if !service.PlanUpdatable {
		return brokerapi.UpdateServiceSpec{}, brokerapi.ErrPlanChangeNotSupported
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	previousServicePlan, ok := b.catalog.FindServicePlan(details.PreviousValues.PlanID)
	if !ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PreviousValues.PlanID)
	}

	if servicePlan.RDSProperties.StorageEncrypted != previousServicePlan.RDSProperties.StorageEncrypted {
		return brokerapi.UpdateServiceSpec{}, ErrEncryptionNotUpdateable
	}

	if servicePlan.RDSProperties.KmsKeyID != previousServicePlan.RDSProperties.KmsKeyID {
		return brokerapi.UpdateServiceSpec{}, ErrEncryptionNotUpdateable
	}

	modifyDBInstance := b.modifyDBInstance(instanceID, servicePlan, updateParameters, details)
	if err := b.dbInstance.Modify(b.dbInstanceIdentifier(instanceID), *modifyDBInstance, updateParameters.ApplyImmediately); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.UpdateServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.UpdateServiceSpec{}, err
	}

	return brokerapi.UpdateServiceSpec{IsAsync: true}, nil
}

func (b *RDSBroker) Deprovision(
	ctx context.Context,
	instanceID string,
	details brokerapi.DeprovisionDetails,
	asyncAllowed bool,
) (brokerapi.DeprovisionServiceSpec, error) {
	b.logger.Debug("deprovision", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	if !asyncAllowed {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return brokerapi.DeprovisionServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	skipDBInstanceFinalSnapshot := servicePlan.RDSProperties.SkipFinalSnapshot

	skipFinalSnapshot, err := b.dbInstance.GetTag(b.dbInstanceIdentifier(instanceID), TagSkipFinalSnapshot)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	if skipFinalSnapshot != "" {
		skipDBInstanceFinalSnapshot, err = strconv.ParseBool(skipFinalSnapshot)
		if err != nil {
			return brokerapi.DeprovisionServiceSpec{}, err
		}
	}

	if err := b.dbInstance.Delete(b.dbInstanceIdentifier(instanceID), skipDBInstanceFinalSnapshot); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	return brokerapi.DeprovisionServiceSpec{IsAsync: true}, nil
}

func (b *RDSBroker) Bind(
	ctx context.Context,
	instanceID, bindingID string,
	details brokerapi.BindDetails,
) (brokerapi.Binding, error) {
	b.logger.Debug("bind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	bindingResponse := brokerapi.Binding{}

	bindParameters := BindParameters{}
	if b.allowUserBindParameters && len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &bindParameters); err != nil {
			return bindingResponse, err
		}
	}

	_, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return bindingResponse, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return bindingResponse, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return bindingResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return bindingResponse, err
	}

	dbAddress := dbInstanceDetails.Address
	dbPort := dbInstanceDetails.Port
	masterUsername := dbInstanceDetails.MasterUsername
	dbName := b.dbNameFromDetails(instanceID, dbInstanceDetails)

	sqlEngine, err := b.sqlProvider.GetSQLEngine(servicePlan.RDSProperties.Engine)
	if err != nil {
		return bindingResponse, err
	}

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.generateMasterPassword(instanceID)); err != nil {
		return bindingResponse, err
	}
	defer sqlEngine.Close()

	dbUsername, dbPassword, err := sqlEngine.CreateUser(bindingID, dbName)
	if err != nil {
		return bindingResponse, err
	}

	bindingResponse.Credentials = Credentials{
		Host:     dbAddress,
		Port:     dbPort,
		Name:     dbName,
		Username: dbUsername,
		Password: dbPassword,
		URI:      sqlEngine.URI(dbAddress, dbPort, dbName, dbUsername, dbPassword),
		JDBCURI:  sqlEngine.JDBCURI(dbAddress, dbPort, dbName, dbUsername, dbPassword),
	}

	return bindingResponse, nil
}

func (b *RDSBroker) Unbind(
	ctx context.Context,
	instanceID, bindingID string,
	details brokerapi.UnbindDetails,
) error {
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

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.generateMasterPassword(instanceID)); err != nil {
		return err
	}
	defer sqlEngine.Close()

	if err = sqlEngine.DropUser(bindingID); err != nil {
		return err
	}

	return nil
}

func (b *RDSBroker) LastOperation(
	ctx context.Context,
	instanceID, operationData string,
) (brokerapi.LastOperation, error) {
	b.logger.Debug("last-operation", lager.Data{
		instanceIDLogKey: instanceID,
	})

	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			err = brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.LastOperation{State: brokerapi.Failed}, err
	}

	state := rdsStatus2State[dbInstanceDetails.Status]
	if state == "" {
		state = brokerapi.Failed
	}

	lastOperationResponse := brokerapi.LastOperation{
		State:       state,
		Description: fmt.Sprintf("DB Instance '%s' status is '%s'", b.dbInstanceIdentifier(instanceID), dbInstanceDetails.Status),
	}

	if lastOperationResponse.State == brokerapi.Succeeded {
		if dbInstanceDetails.PendingModifications {
			lastOperationResponse = brokerapi.LastOperation{
				State:       brokerapi.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' has pending modifications", b.dbInstanceIdentifier(instanceID)),
			}
		} else {
			asyncOperarionTriggered, err := b.PostRestoreTasks(instanceID, &dbInstanceDetails)
			if err != nil {
				return brokerapi.LastOperation{State: brokerapi.Failed}, err
			}
			if asyncOperarionTriggered {
				lastOperationResponse = brokerapi.LastOperation{
					State:       brokerapi.InProgress,
					Description: fmt.Sprintf("DB Instance '%s' has pending post restore modifications", b.dbInstanceIdentifier(instanceID)),
				}
			}
		}
	}

	b.logger.Debug("last-operation.done", lager.Data{
		instanceIDLogKey:            instanceID,
		lastOperationResponseLogKey: lastOperationResponse,
	})

	return lastOperationResponse, nil
}

func (b *RDSBroker) updateDBSettings(instanceID string, dbInstanceDetails *awsrds.DBInstanceDetails) (asyncOperarionTriggered bool, err error) {
	serviceID := dbInstanceDetails.Tags[TagServiceID]
	planID := dbInstanceDetails.Tags[TagPlanID]
	organizationID := dbInstanceDetails.Tags[TagOrganizationID]
	spaceID := dbInstanceDetails.Tags[TagSpaceID]

	servicePlan, ok := b.catalog.FindServicePlan(planID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", dbInstanceDetails.Tags[TagPlanID])
	}

	newDbInstanceDetails := b.dbInstanceFromPlan(servicePlan)
	newDbInstanceDetails.Tags = b.dbTags("Restored", serviceID, planID, organizationID, spaceID, "", "")
	newDbInstanceDetails.MasterUserPassword = b.generateMasterPassword(instanceID)
	if err := b.dbInstance.Modify(b.dbInstanceIdentifier(instanceID), *newDbInstanceDetails, true); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return false, brokerapi.ErrInstanceDoesNotExist
		}
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) rebootInstance(instanceID string, dbInstanceDetails *awsrds.DBInstanceDetails) (asyncOperarionTriggered bool, err error) {
	err = b.dbInstance.Reboot(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) changeUserPassword(instanceID string, dbInstanceDetails *awsrds.DBInstanceDetails) (asyncOperarionTriggered bool, err error) {
	dbAddress := dbInstanceDetails.Address
	dbPort := dbInstanceDetails.Port
	masterUsername := dbInstanceDetails.MasterUsername
	dbName := b.dbNameFromDetails(instanceID, *dbInstanceDetails)

	sqlEngine, err := b.sqlProvider.GetSQLEngine(dbInstanceDetails.Engine)
	if err != nil {
		return false, err
	}

	if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.generateMasterPassword(instanceID)); err != nil {
		return false, err
	}
	defer sqlEngine.Close()

	err = sqlEngine.ResetState()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) PostRestoreTasks(instanceID string, dbInstanceDetails *awsrds.DBInstanceDetails) (asyncOperarionTriggered bool, err error) {
	var restoreStateFuncs = map[string]func(string, *awsrds.DBInstanceDetails) (bool, error){
		StateUpdateSettings:    b.updateDBSettings,
		StateReboot:            b.rebootInstance,
		StateResetUserPassword: b.changeUserPassword,
	}

	for _, state := range restoreStateSequence {
		_, tag := dbInstanceDetails.Tags[state]
		if tag {
			b.logger.Debug(fmt.Sprintf("last-operation.%s", state))
			var success, err = restoreStateFuncs[state](instanceID, dbInstanceDetails)
			if success {
				var err = b.dbInstance.RemoveTag(b.dbInstanceIdentifier(instanceID), state)
				if err != nil {
					return false, err
				}
			}
			return success, err
		}
	}

	return false, nil
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
		masterPassword := b.generateMasterPassword(serviceInstanceID)
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

func (b *RDSBroker) MigrateToEventTriggers() {

	dbInstanceDetailsList, err := b.dbInstance.DescribeByTag("Broker Name", b.brokerName)
	if err != nil {
		b.logger.Error("Could not obtain the list of instances", err)
		return
	}

	for _, dbDetails := range dbInstanceDetailsList {
		b.logger.Info(fmt.Sprintf("Started migration of %s to use event triggers", b.dbInstance))
		serviceInstanceID := b.dbInstanceIdentifierToServiceInstanceID(dbDetails.Identifier)
		masterPassword := b.generateMasterPassword(serviceInstanceID)
		dbname := b.dbNameFromDetails(dbDetails.Identifier, *dbDetails)
		sengine, err := b.sqlProvider.GetSQLEngine(dbDetails.Engine)
		if err != nil {
			b.logger.Error(fmt.Sprintf("Could not determine SQL Engine of instance %v", dbDetails.Identifier), err)
			continue
		}
		if dbDetails.Engine == "postgres" {
			continue
		}

		pgEngine, ok := sengine.(*sqlengine.PostgresEngine)
		if !ok {
			pgEngine.Migrate(dbDetails, dbname, masterPassword)
		}

	}
}

func (b *RDSBroker) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", strings.Replace(b.dbPrefix, "_", "-", -1), strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSBroker) dbInstanceIdentifierToServiceInstanceID(serviceInstanceID string) string {
	return strings.TrimPrefix(serviceInstanceID, strings.Replace(b.dbPrefix, "_", "-", -1)+"-")
}

func (b *RDSBroker) generateMasterUsername() string {
	return utils.RandomAlphaNum(masterUsernameLength)
}

func (b *RDSBroker) generateMasterPassword(instanceID string) string {
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

	dbInstanceDetails.MasterUsername = b.generateMasterUsername()
	dbInstanceDetails.MasterUserPassword = b.generateMasterPassword(instanceID)

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

	dbInstanceDetails.Tags = b.dbTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshot, "")
	return dbInstanceDetails
}

func (b *RDSBroker) restoreDBInstance(instanceID, snapshotIdentifier string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) *awsrds.DBInstanceDetails {
	dbInstanceDetails := b.dbInstanceFromPlan(servicePlan)
	skipFinalSnapshot := strconv.FormatBool(servicePlan.RDSProperties.SkipFinalSnapshot)
	if provisionParameters.SkipFinalSnapshot != "" {
		skipFinalSnapshot = provisionParameters.SkipFinalSnapshot
	}

	dbInstanceDetails.Tags = b.dbTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshot, snapshotIdentifier)
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

	dbInstanceDetails.Tags = b.dbTags("Updated", details.ServiceID, details.PlanID, "", "", skipFinalSnapshot, "")

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

func (b *RDSBroker) dbTags(action, serviceID, planID, organizationID, spaceID, skipFinalSnapshot, originSnapshotIdentifier string) map[string]string {
	tags := make(map[string]string)

	tags["Owner"] = "Cloud Foundry"

	tags["Broker Name"] = b.brokerName

	tags[action+" by"] = "AWS RDS Service Broker"

	tags[action+" at"] = time.Now().Format(time.RFC822Z)

	if serviceID != "" {
		tags[TagServiceID] = serviceID
	}

	if planID != "" {
		tags[TagPlanID] = planID
	}

	if organizationID != "" {
		tags[TagOrganizationID] = organizationID
	}

	if spaceID != "" {
		tags[TagSpaceID] = spaceID
	}

	if skipFinalSnapshot != "" {
		tags[TagSkipFinalSnapshot] = skipFinalSnapshot
	}

	if originSnapshotIdentifier != "" {
		tags[TagRestoredFromSnapshot] = originSnapshotIdentifier
		for _, state := range restoreStateSequence {
			tags[state] = "true"
		}
	}
	return tags
}
