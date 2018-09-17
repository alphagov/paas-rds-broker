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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
)

const MasterUsernameLength = 16
const MasterPasswordLength = 32

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
	"available":                           brokerapi.Succeeded,
	"backing-up":                          brokerapi.InProgress,
	"creating":                            brokerapi.InProgress,
	"deleting":                            brokerapi.InProgress,
	"maintenance":                         brokerapi.InProgress,
	"modifying":                           brokerapi.InProgress,
	"rebooting":                           brokerapi.InProgress,
	"renaming":                            brokerapi.InProgress,
	"resetting-master-credentials":        brokerapi.InProgress,
	"upgrading":                           brokerapi.InProgress,
	"configuring-enhanced-monitoring":     brokerapi.InProgress,
	"starting":                            brokerapi.InProgress,
	"stopping":                            brokerapi.InProgress,
	"stopped":                             brokerapi.InProgress,
	"storage-full":                        brokerapi.InProgress,
	"storage-optimization":                brokerapi.InProgress,
	"failed":                              brokerapi.Failed,
	"incompatible-credentials":            brokerapi.Failed,
	"incompatible-network":                brokerapi.Failed,
	"incompatible-option-group":           brokerapi.Failed,
	"incompatible-parameters":             brokerapi.Failed,
	"incompatible-restore":                brokerapi.Failed,
	"restore-error":                       brokerapi.Failed,
	"inaccessible-encryption-credentials": brokerapi.Failed,
}

const StateUpdateSettings = "PendingUpdateSettings"
const StateReboot = "PendingReboot"
const StateResetUserPassword = "PendingResetUserPassword"

var restoreStateSequence = []string{StateUpdateSettings, StateReboot, StateResetUserPassword}

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
		if err := b.dbInstance.Create(createDBInstance); err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
	} else {
		if *provisionParameters.RestoreFromLatestSnapshotOf == "" {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Invalid guid: '%s'", *provisionParameters.RestoreFromLatestSnapshotOf)
		}
		if servicePlan.RDSProperties.Engine != nil && *servicePlan.RDSProperties.Engine != "postgres" {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Restore from snapshot not supported for engine '%s'", *servicePlan.RDSProperties.Engine)
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

		tags, err := b.dbInstance.GetSnapshotTags(snapshot)
		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
		tagsByName := awsrds.RDSTagsValues(tags)

		if tagsByName[awsrds.TagSpaceID] != details.SpaceGUID || tagsByName[awsrds.TagOrganizationID] != details.OrganizationGUID {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("The service instance you are getting a snapshot from is not in the same org or space")
		}
		if tagsByName[awsrds.TagPlanID] != details.PlanID {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("You must use the same plan as the service instance you are getting a snapshot from")
		}
		snapshotIdentifier := aws.StringValue(snapshot.DBSnapshotIdentifier)
		restoreDBInstanceInput := b.restoreDBInstanceInput(instanceID, snapshotIdentifier, servicePlan, provisionParameters, details)
		if err := b.dbInstance.Restore(restoreDBInstanceInput); err != nil {
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

	modifyDBInstanceInput, tags := b.newModifyDBInstanceInputAndTags(instanceID, servicePlan, updateParameters, details)
	if err := b.dbInstance.Modify(modifyDBInstanceInput, tags); err != nil {
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

	skipDBInstanceFinalSnapshot := servicePlan.RDSProperties.SkipFinalSnapshot == nil && *servicePlan.RDSProperties.SkipFinalSnapshot

	skipFinalSnapshot, err := b.dbInstance.GetTag(b.dbInstanceIdentifier(instanceID), awsrds.TagSkipFinalSnapshot)
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

	var (
		dbAddress string
		dbPort    int64
	)
	if dbInstanceDetails.Endpoint == nil {
		dbAddress = ""
	} else {
		dbAddress = aws.StringValue(dbInstanceDetails.Endpoint.Address)
		dbPort = aws.Int64Value(dbInstanceDetails.Endpoint.Port)
	}
	masterUsername := aws.StringValue(dbInstanceDetails.MasterUsername)
	dbName := b.dbNameFromDBInstance(instanceID, dbInstanceDetails)

	var engine string
	if servicePlan.RDSProperties.Engine != nil {
		engine = *servicePlan.RDSProperties.Engine
	}
	sqlEngine, err := b.sqlProvider.GetSQLEngine(engine)
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

	dbInstanceDetails, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.ErrInstanceDoesNotExist
		}
		return err
	}

	var (
		dbAddress string
		dbPort    int64
	)
	if dbInstanceDetails.Endpoint == nil {
		dbAddress = ""
	} else {
		dbAddress = aws.StringValue(dbInstanceDetails.Endpoint.Address)
		dbPort = aws.Int64Value(dbInstanceDetails.Endpoint.Port)
	}
	masterUsername := aws.StringValue(dbInstanceDetails.MasterUsername)
	dbName := b.dbNameFromDBInstance(instanceID, dbInstanceDetails)

	var engine string
	if servicePlan.RDSProperties.Engine != nil {
		engine = *servicePlan.RDSProperties.Engine
	}
	sqlEngine, err := b.sqlProvider.GetSQLEngine(engine)
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

	tags, err := b.dbInstance.GetDBInstanceTags(dbInstanceDetails, awsrds.DescribeRefreshCacheOption)
	if err != nil {
		return brokerapi.LastOperation{State: brokerapi.Failed}, err
	}

	tagsByName := awsrds.RDSTagsValues(tags)

	status := aws.StringValue(dbInstanceDetails.DBInstanceStatus)
	state, ok := rdsStatus2State[status]
	if !ok {
		state = brokerapi.InProgress
	}

	lastOperationResponse := brokerapi.LastOperation{
		State:       state,
		Description: fmt.Sprintf("DB Instance '%s' status is '%s'", b.dbInstanceIdentifier(instanceID), status),
	}

	if lastOperationResponse.State == brokerapi.Succeeded {
		if dbInstanceDetails.PendingModifiedValues != nil {
			lastOperationResponse = brokerapi.LastOperation{
				State:       brokerapi.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' has pending modifications", b.dbInstanceIdentifier(instanceID)),
			}
		} else {
			asyncOperarionTriggered, err := b.PostRestoreTasks(instanceID, dbInstanceDetails, tagsByName)
			if err != nil {
				return brokerapi.LastOperation{State: brokerapi.Failed}, err
			}
			if asyncOperarionTriggered {
				lastOperationResponse = brokerapi.LastOperation{
					State:       brokerapi.InProgress,
					Description: fmt.Sprintf("DB Instance '%s' has pending post restore modifications", b.dbInstanceIdentifier(instanceID)),
				}
			} else {
				err = b.ensureCreateExtensions(instanceID, dbInstanceDetails, tagsByName)
				if err != nil {
					return brokerapi.LastOperation{State: brokerapi.Failed}, err
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

func (b *RDSBroker) ensureCreateExtensions(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) error {
	b.logger.Debug("ensure-create-extensions", lager.Data{
		instanceIDLogKey: instanceID,
	})

	planID := tagsByName[awsrds.TagPlanID]
	servicePlan, ok := b.catalog.FindServicePlan(planID)
	if !ok {
		return fmt.Errorf("Service Plan '%s' not found while ensuring extensions are created", planID)
	}

	if engine := servicePlan.RDSProperties.Engine; engine != nil && *engine == "postgres" {
		var (
			dbAddress string
			dbPort    int64
		)
		if dbInstance.Endpoint == nil {
			dbAddress = ""
		} else {
			dbAddress = aws.StringValue(dbInstance.Endpoint.Address)
			dbPort = aws.Int64Value(dbInstance.Endpoint.Port)
		}
		masterUsername := aws.StringValue(dbInstance.MasterUsername)
		dbName := b.dbNameFromDBInstance(instanceID, dbInstance)

		sqlEngine, err := b.sqlProvider.GetSQLEngine(*engine)
		if err != nil {
			return err
		}

		if err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.generateMasterPassword(instanceID)); err != nil {
			return err
		}
		defer sqlEngine.Close()

		postgresExtensionsString := []string{}
		for _, extension := range servicePlan.RDSProperties.PostgresExtensions {
			if extension != nil {
				postgresExtensionsString = append(postgresExtensionsString, *extension)
			}
		}

		if err = sqlEngine.CreateExtensions(postgresExtensionsString); err != nil {
			return err
		}
	}

	return nil
}

func (b *RDSBroker) updateDBSettings(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperarionTriggered bool, err error) {
	serviceID := tagsByName[awsrds.TagServiceID]
	planID := tagsByName[awsrds.TagPlanID]
	organizationID := tagsByName[awsrds.TagOrganizationID]
	spaceID := tagsByName[awsrds.TagSpaceID]

	servicePlan, ok := b.catalog.FindServicePlan(planID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", tagsByName[awsrds.TagPlanID])
	}

	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan)
	tags := b.dbTags("Restored", serviceID, planID, organizationID, spaceID, "", "")
	rdsTags := awsrds.BuilRDSTags(tags)
	modifyDBInstanceInput.MasterUserPassword = aws.String(b.generateMasterPassword(instanceID))
	if err := b.dbInstance.Modify(modifyDBInstanceInput, rdsTags); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return false, brokerapi.ErrInstanceDoesNotExist
		}
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) rebootInstance(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperarionTriggered bool, err error) {
	err = b.dbInstance.Reboot(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) changeUserPassword(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperarionTriggered bool, err error) {
	var (
		dbAddress string
		dbPort    int64
	)
	if dbInstance.Endpoint == nil {
		dbAddress = ""
	} else {
		dbAddress = aws.StringValue(dbInstance.Endpoint.Address)
		dbPort = aws.Int64Value(dbInstance.Endpoint.Port)
	}
	masterUsername := aws.StringValue(dbInstance.MasterUsername)
	dbName := b.dbNameFromDBInstance(instanceID, dbInstance)

	var engine string
	if dbInstance.Engine != nil {
		engine = *dbInstance.Engine
	}
	sqlEngine, err := b.sqlProvider.GetSQLEngine(engine)
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

func (b *RDSBroker) PostRestoreTasks(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperarionTriggered bool, err error) {
	restoreStateFuncs := map[string]func(instanceID string, instance *rds.DBInstance, tagsByName map[string]string) (bool, error){
		StateUpdateSettings:    b.updateDBSettings,
		StateReboot:            b.rebootInstance,
		StateResetUserPassword: b.changeUserPassword,
	}

	for _, state := range restoreStateSequence {
		_, tag := tagsByName[state]
		if tag {
			b.logger.Debug(fmt.Sprintf("last-operation.%s", state))
			var success, err = restoreStateFuncs[state](instanceID, dbInstance, tagsByName)
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

	for _, dbInstance := range dbInstanceDetailsList {
		dbInstanceIdentifier := aws.StringValue(dbInstance.DBInstanceIdentifier)
		b.logger.Debug(fmt.Sprintf("Checking credentials for instance %v", dbInstanceIdentifier))
		serviceInstanceID := b.dbInstanceIdentifierToServiceInstanceID(dbInstanceIdentifier)
		masterPassword := b.generateMasterPassword(serviceInstanceID)
		dbName := b.dbNameFromDBInstance(dbInstanceIdentifier, dbInstance)

		sqlEngine, err := b.sqlProvider.GetSQLEngine(aws.StringValue(dbInstance.Engine))
		if err != nil {
			b.logger.Error(fmt.Sprintf("Could not determine SQL Engine of instance %v", dbInstanceIdentifier), err)
			continue
		}

		var (
			dbAddress string
			dbPort    int64
		)
		if dbInstance.Endpoint == nil {
			dbAddress = ""
		} else {
			dbAddress = aws.StringValue(dbInstance.Endpoint.Address)
			dbPort = aws.Int64Value(dbInstance.Endpoint.Port)
		}
		masterUsername := aws.StringValue(dbInstance.MasterUsername)
		err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, masterPassword)
		sqlEngine.Close()

		if err != nil {
			if err == sqlengine.LoginFailedError {
				b.logger.Info(fmt.Sprintf(
					"Login failed when connecting to DB %v at %v. Will attempt to reset the password.",
					dbName, dbAddress))
				changePasswordInput := &rds.ModifyDBInstanceInput{
					DBInstanceIdentifier: dbInstance.DBInstanceIdentifier,
					MasterUserPassword:   aws.String(masterPassword),
				}
				err = b.dbInstance.Modify(changePasswordInput, []*rds.Tag{})
				if err != nil {
					b.logger.Error(fmt.Sprintf("Could not reset the master password of instance %v", dbInstanceIdentifier), err)
				}
			} else {
				b.logger.Error(fmt.Sprintf("Unknown error when connecting to DB %v at %v", dbName, dbAddress), err)
			}
		}
	}
	b.logger.Info(fmt.Sprintf("Instances credentials check has ended"))
}

func (b *RDSBroker) dbInstanceIdentifier(instanceID string) string {
	return fmt.Sprintf("%s-%s", strings.Replace(b.dbPrefix, "_", "-", -1), strings.Replace(instanceID, "_", "-", -1))
}

func (b *RDSBroker) dbInstanceIdentifierToServiceInstanceID(serviceInstanceID string) string {
	return strings.TrimPrefix(serviceInstanceID, strings.Replace(b.dbPrefix, "_", "-", -1)+"-")
}

func (b *RDSBroker) generateMasterUsername() string {
	return utils.RandomAlphaNum(MasterUsernameLength)
}

func (b *RDSBroker) generateMasterPassword(instanceID string) string {
	return utils.GenerateHash(b.masterPasswordSeed+instanceID, MasterPasswordLength)
}

func (b *RDSBroker) dbName(instanceID string) string {
	return fmt.Sprintf("%s_%s", strings.Replace(b.dbPrefix, "-", "_", -1), strings.Replace(instanceID, "-", "_", -1))
}

func (b *RDSBroker) dbNameFromDBInstance(instanceID string, dbInstance *rds.DBInstance) string {
	var dbName string
	dbNameString := aws.StringValue(dbInstance.DBName)
	if dbNameString != "" {
		dbName = dbNameString
	} else {
		dbName = b.dbName(instanceID)
	}
	return dbName
}

func (b *RDSBroker) createDBInstance(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) *rds.CreateDBInstanceInput {
	var skipFinalSnapshot string
	if provisionParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = strconv.FormatBool(*provisionParameters.SkipFinalSnapshot)
	} else {
		skipFinalSnapshot = strconv.FormatBool(*servicePlan.RDSProperties.SkipFinalSnapshot)
	}
	return &rds.CreateDBInstanceInput{
		DBName:                     aws.String(b.dbName(instanceID)),
		MasterUsername:             aws.String(b.generateMasterUsername()),
		MasterUserPassword:         aws.String(b.generateMasterPassword(instanceID)),
		DBInstanceClass:            servicePlan.RDSProperties.DBInstanceClass,
		Engine:                     servicePlan.RDSProperties.Engine,
		AutoMinorVersionUpgrade:    servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		AvailabilityZone:           servicePlan.RDSProperties.AvailabilityZone,
		CopyTagsToSnapshot:         servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:       servicePlan.RDSProperties.DBParameterGroupName,
		DBSubnetGroupName:          servicePlan.RDSProperties.DBSubnetGroupName,
		EngineVersion:              servicePlan.RDSProperties.EngineVersion,
		OptionGroupName:            servicePlan.RDSProperties.OptionGroupName,
		PreferredMaintenanceWindow: servicePlan.RDSProperties.PreferredMaintenanceWindow,
		PubliclyAccessible:         servicePlan.RDSProperties.PubliclyAccessible,
		BackupRetentionPeriod:      servicePlan.RDSProperties.BackupRetentionPeriod,
		AllocatedStorage:           servicePlan.RDSProperties.AllocatedStorage,
		CharacterSetName:           servicePlan.RDSProperties.CharacterSetName,
		DBSecurityGroups:           servicePlan.RDSProperties.DBSecurityGroups,
		Iops:                       servicePlan.RDSProperties.Iops,
		KmsKeyId:                   servicePlan.RDSProperties.KmsKeyID,
		LicenseModel:               servicePlan.RDSProperties.LicenseModel,
		MultiAZ:                    servicePlan.RDSProperties.MultiAZ,
		Port:                       servicePlan.RDSProperties.Port,
		PreferredBackupWindow: servicePlan.RDSProperties.PreferredBackupWindow,
		StorageEncrypted:      servicePlan.RDSProperties.StorageEncrypted,
		StorageType:           servicePlan.RDSProperties.StorageType,
		VpcSecurityGroupIds:   servicePlan.RDSProperties.VpcSecurityGroupIds,
		Tags:                  awsrds.BuilRDSTags(b.dbTags("Created", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshot, "")),
	}
}

func (b *RDSBroker) restoreDBInstanceInput(instanceID, snapshotIdentifier string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) *rds.RestoreDBInstanceFromDBSnapshotInput {
	var skipFinalSnapshot string
	if provisionParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = strconv.FormatBool(*provisionParameters.SkipFinalSnapshot)
	} else {
		skipFinalSnapshot = strconv.FormatBool(*servicePlan.RDSProperties.SkipFinalSnapshot)
	}
	return &rds.RestoreDBInstanceFromDBSnapshotInput{
		DBSnapshotIdentifier:    aws.String(snapshotIdentifier),
		DBInstanceIdentifier:    aws.String(instanceID),
		DBInstanceClass:         servicePlan.RDSProperties.DBInstanceClass,
		Engine:                  servicePlan.RDSProperties.Engine,
		AutoMinorVersionUpgrade: servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		AvailabilityZone:        servicePlan.RDSProperties.AvailabilityZone,
		CopyTagsToSnapshot:      servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBSubnetGroupName:       servicePlan.RDSProperties.DBSubnetGroupName,
		OptionGroupName:         servicePlan.RDSProperties.OptionGroupName,
		PubliclyAccessible:      servicePlan.RDSProperties.PubliclyAccessible,
		Iops:                    servicePlan.RDSProperties.Iops,
		LicenseModel:            servicePlan.RDSProperties.LicenseModel,
		MultiAZ:                 servicePlan.RDSProperties.MultiAZ,
		Port:                    servicePlan.RDSProperties.Port,
		StorageType:             servicePlan.RDSProperties.StorageType,
		Tags:                    awsrds.BuilRDSTags(b.dbTags("Restored", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshot, snapshotIdentifier)),
	}
}

func (b *RDSBroker) newModifyDBInstanceInput(instanceID string, servicePlan ServicePlan) *rds.ModifyDBInstanceInput {
	modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceClass:            servicePlan.RDSProperties.DBInstanceClass,
		AutoMinorVersionUpgrade:    servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		CopyTagsToSnapshot:         servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:       servicePlan.RDSProperties.DBParameterGroupName,
		DBSubnetGroupName:          servicePlan.RDSProperties.DBSubnetGroupName,
		EngineVersion:              servicePlan.RDSProperties.EngineVersion,
		OptionGroupName:            servicePlan.RDSProperties.OptionGroupName,
		PreferredMaintenanceWindow: servicePlan.RDSProperties.PreferredMaintenanceWindow,
		PubliclyAccessible:         servicePlan.RDSProperties.PubliclyAccessible,
		BackupRetentionPeriod:      servicePlan.RDSProperties.BackupRetentionPeriod,
		AllocatedStorage:           servicePlan.RDSProperties.AllocatedStorage,
		DBSecurityGroups:           servicePlan.RDSProperties.DBSecurityGroups,
		Iops:                       servicePlan.RDSProperties.Iops,
		LicenseModel:               servicePlan.RDSProperties.LicenseModel,
		MultiAZ:                    servicePlan.RDSProperties.MultiAZ,
		PreferredBackupWindow:      servicePlan.RDSProperties.PreferredBackupWindow,
		StorageType:                servicePlan.RDSProperties.StorageType,
		VpcSecurityGroupIds:        servicePlan.RDSProperties.VpcSecurityGroupIds,
	}

	b.logger.Debug("newModifyDBInstanceInputAndTags", lager.Data{
		instanceIDLogKey:        instanceID,
		servicePlanLogKey:       servicePlan,
		dbInstanceDetailsLogKey: modifyDBInstanceInput,
	})

	return modifyDBInstanceInput

}

func (b *RDSBroker) newModifyDBInstanceInputAndTags(instanceID string, servicePlan ServicePlan, updateParameters UpdateParameters, details brokerapi.UpdateDetails) (*rds.ModifyDBInstanceInput, []*rds.Tag) {
	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan)
	modifyDBInstanceInput.ApplyImmediately = aws.Bool(!updateParameters.ApplyAtMaintenanceWindow)

	var skipFinalSnapshot string
	if updateParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = strconv.FormatBool(*updateParameters.SkipFinalSnapshot)
	} else {
		skipFinalSnapshot = strconv.FormatBool(servicePlan.RDSProperties.SkipFinalSnapshot == nil && *servicePlan.RDSProperties.SkipFinalSnapshot)
	}

	tags := awsrds.BuilRDSTags(b.dbTags("Updated", details.ServiceID, details.PlanID, "", "", skipFinalSnapshot, ""))
	return modifyDBInstanceInput, tags
}

func (b *RDSBroker) dbTags(action, serviceID, planID, organizationID, spaceID, skipFinalSnapshot, originSnapshotIdentifier string) map[string]string {
	tags := make(map[string]string)

	tags["Owner"] = "Cloud Foundry"

	tags[awsrds.TagBrokerName] = b.brokerName

	tags[action+" by"] = "AWS RDS Service Broker"

	tags[action+" at"] = time.Now().Format(time.RFC822Z)

	if serviceID != "" {
		tags[awsrds.TagServiceID] = serviceID
	}

	if planID != "" {
		tags[awsrds.TagPlanID] = planID
	}

	if organizationID != "" {
		tags[awsrds.TagOrganizationID] = organizationID
	}

	if spaceID != "" {
		tags[awsrds.TagSpaceID] = spaceID
	}

	if skipFinalSnapshot != "" {
		tags[awsrds.TagSkipFinalSnapshot] = skipFinalSnapshot
	}

	if originSnapshotIdentifier != "" {
		tags[awsrds.TagRestoredFromSnapshot] = originSnapshotIdentifier
		for _, state := range restoreStateSequence {
			tags[state] = "true"
		}
	}
	return tags
}
