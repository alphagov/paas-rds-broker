package rdsbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
const dbInstanceLogKey = "dbInstance"
const lastOperationResponseLogKey = "lastOperationResponse"
const extensionsLogKey = "requestedExtensions"

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
	dbInstance                   awsrds.RDSInstance
	sqlProvider                  sqlengine.Provider
	logger                       lager.Logger
	brokerName                   string
	parameterGroupsSelector      ParameterGroupSelector
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

type RDSInstanceTags struct {
	Action                   string
	ServiceID                string
	PlanID                   string
	OrganizationID           string
	SpaceID                  string
	SkipFinalSnapshot        string
	OriginSnapshotIdentifier string
	Extensions               []string
}

func New(
	config Config,
	dbInstance awsrds.RDSInstance,
	sqlProvider sqlengine.Provider,
	parameterGroupSelector ParameterGroupSelector,
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
		parameterGroupsSelector:      parameterGroupSelector,
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

	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" {
		ensureDefaultExtensionsAreSet(&provisionParameters, servicePlan)
		ok, unsupportedExtensions := extensionsAreSupported(servicePlan, provisionParameters)
		if !ok {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("%s is not supported", unsupportedExtensions)
		}
	}

	if provisionParameters.RestoreFromLatestSnapshotOf == nil {
		createDBInstance, err := b.createDBInstance(instanceID, servicePlan, provisionParameters, details)
		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
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

		tags, err := b.dbInstance.GetResourceTags(aws.StringValue(snapshot.DBSnapshotArn))
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

		if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
			if extensionsTag != "" {
				snapshotExts := strings.Split(extensionsTag, ":")
				ensureExtensionsAreSet(&provisionParameters, snapshotExts)
			}
		}

		restoreDBInstanceInput, err := b.restoreDBInstanceInput(instanceID, snapshotIdentifier, servicePlan, provisionParameters, details)

		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}

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

	b.logger.Info("update", lager.Data{"instanceID": instanceID, "details": details})

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

	if updateParameters.Reboot != nil && *updateParameters.Reboot {
		if details.PlanID != details.PreviousValues.PlanID {
			return brokerapi.UpdateServiceSpec{}, fmt.Errorf("Invalid to reboot and update plan in the same command")
		}

		rebootDBInstanceInput := &rds.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
			ForceFailover:        updateParameters.ForceFailover,
		}

		err := b.dbInstance.Reboot(rebootDBInstanceInput)
		if err != nil {
			if err == awsrds.ErrDBInstanceDoesNotExist {
				return brokerapi.UpdateServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
			}
			return brokerapi.UpdateServiceSpec{}, err
		}
		return brokerapi.UpdateServiceSpec{IsAsync: true}, nil
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

	if !reflect.DeepEqual(servicePlan.RDSProperties.StorageEncrypted, previousServicePlan.RDSProperties.StorageEncrypted) {
		return brokerapi.UpdateServiceSpec{}, ErrEncryptionNotUpdateable
	}

	if !reflect.DeepEqual(servicePlan.RDSProperties.KmsKeyID, previousServicePlan.RDSProperties.KmsKeyID) {
		return brokerapi.UpdateServiceSpec{}, ErrEncryptionNotUpdateable
	}

	existingInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))

	if err != nil {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("cannot find instance %s", b.dbInstanceIdentifier(instanceID))
	}

	// The db parameter group should not change
	// when updating a database, because it controls
	// the function of certain extensions which
	// could have been enabled at creation time
	previousDbParamGroup := existingInstance.DBParameterGroups[0].DBParameterGroupName

	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan, previousDbParamGroup)
	modifyDBInstanceInput.ApplyImmediately = aws.Bool(!updateParameters.ApplyAtMaintenanceWindow)

	updatedDBInstance, err := b.dbInstance.Modify(modifyDBInstanceInput)
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.UpdateServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.UpdateServiceSpec{}, err
	}

	instanceTags := RDSInstanceTags{
		Action:    "Updated",
		ServiceID: details.ServiceID,
		PlanID:    details.PlanID,
	}
	if updateParameters.SkipFinalSnapshot != nil {
		instanceTags.SkipFinalSnapshot = strconv.FormatBool(*updateParameters.SkipFinalSnapshot)
	}

	tags := awsrds.BuilRDSTags(b.dbTags(instanceTags))
	b.dbInstance.AddTagsToResource(aws.StringValue(updatedDBInstance.DBInstanceArn), tags)

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

	skipDBInstanceFinalSnapshot := servicePlan.RDSProperties.SkipFinalSnapshot == nil || *servicePlan.RDSProperties.SkipFinalSnapshot

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

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return bindingResponse, brokerapi.ErrInstanceDoesNotExist
		}
		return bindingResponse, err
	}

	dbAddress := awsrds.GetDBAddress(dbInstance.Endpoint)
	dbPort := awsrds.GetDBPort(dbInstance.Endpoint)
	masterUsername := aws.StringValue(dbInstance.MasterUsername)
	dbName := b.dbNameFromDBInstance(instanceID, dbInstance)

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

	_, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.ErrInstanceDoesNotExist
		}
		return err
	}

	dbName := b.dbNameFromDBInstance(instanceID, dbInstance)
	sqlEngine, err := b.openSQLEngineForDBInstance(instanceID, dbName, dbInstance)
	if err != nil {
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

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			err = brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.LastOperation{State: brokerapi.Failed}, err
	}

	tags, err := b.dbInstance.GetResourceTags(
		aws.StringValue(dbInstance.DBInstanceArn),
		awsrds.DescribeRefreshCacheOption,
	)
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			err = brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.LastOperation{State: brokerapi.Failed}, err
	}

	tagsByName := awsrds.RDSTagsValues(tags)

	status := aws.StringValue(dbInstance.DBInstanceStatus)
	state, ok := rdsStatus2State[status]
	if !ok {
		state = brokerapi.InProgress
	}

	lastOperationResponse := brokerapi.LastOperation{
		State:       state,
		Description: fmt.Sprintf("DB Instance '%s' status is '%s'", b.dbInstanceIdentifier(instanceID), status),
	}

	if lastOperationResponse.State == brokerapi.Succeeded {
		hasPendingModifications := false
		if dbInstance.PendingModifiedValues != nil {
			emptyPendingModifiedValues := rds.PendingModifiedValues{}
			if !reflect.DeepEqual(*dbInstance.PendingModifiedValues, emptyPendingModifiedValues) {
				hasPendingModifications = true
			}
		}
		if hasPendingModifications {
			lastOperationResponse = brokerapi.LastOperation{
				State:       brokerapi.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' has pending modifications", b.dbInstanceIdentifier(instanceID)),
			}
		} else {
			asyncOperarionTriggered, err := b.PostRestoreTasks(instanceID, dbInstance, tagsByName)
			if err != nil {
				return brokerapi.LastOperation{State: brokerapi.Failed}, err
			}
			if asyncOperarionTriggered {
				lastOperationResponse = brokerapi.LastOperation{
					State:       brokerapi.InProgress,
					Description: fmt.Sprintf("DB Instance '%s' has pending post restore modifications", b.dbInstanceIdentifier(instanceID)),
				}
			} else {
				err = b.ensureCreateExtensions(instanceID, dbInstance, tagsByName)
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

func ensureDefaultExtensionsAreSet(parameters *ProvisionParameters, plan ServicePlan) {
	extensions := []string{}
	for _, e := range plan.RDSProperties.DefaultExtensions {
		extensions = append(extensions, aws.StringValue(e))
	}

	ensureExtensionsAreSet(parameters, extensions)
}

func ensureExtensionsAreSet(parameters *ProvisionParameters, extensions []string) {
	inSlice := func(slice []string, element string) bool {
		for _, e := range slice {
			if e == element {
				return true
			}
		}

		return false
	}

	for _, e := range extensions {
		if !inSlice(parameters.Extensions, e) {
			parameters.Extensions = append(parameters.Extensions, e)
		}
	}
}

func extensionsAreSupported(plan ServicePlan, parameters ProvisionParameters) (bool, string) {
	extensions := parameters.Extensions

	supported := plan.RDSProperties.AllowedExtensions
	for _, e := range extensions {
		if !extensionIsSupported(supported, e) {
			return false, e
		}
	}
	return true, ""
}

func extensionIsSupported(extensions []*string, s string) bool {
	for _, e := range extensions {
		if s == *e {
			return true
		}
	}
	return false
}

func (b *RDSBroker) ensureCreateExtensions(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) error {
	b.logger.Debug("ensure-create-extensions", lager.Data{
		instanceIDLogKey: instanceID,
	})

	if aws.StringValue(dbInstance.Engine) == "postgres" {
		dbName := b.dbNameFromDBInstance(instanceID, dbInstance)
		sqlEngine, err := b.openSQLEngineForDBInstance(instanceID, dbName, dbInstance)
		if err != nil {
			return err
		}
		defer sqlEngine.Close()

		if extensions, exists := tagsByName[awsrds.TagExtensions]; exists {
			postgresExtensionsString := strings.Split(extensions, ":")

			if err = sqlEngine.CreateExtensions(postgresExtensionsString); err != nil {
				return err
			}
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

	existingParameterGroup := dbInstance.DBParameterGroups[0].DBParameterGroupName

	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan, existingParameterGroup)
	modifyDBInstanceInput.MasterUserPassword = aws.String(b.generateMasterPassword(instanceID))
	updatedDBInstance, err := b.dbInstance.Modify(modifyDBInstanceInput)
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return false, brokerapi.ErrInstanceDoesNotExist
		}
		return false, err
	}

	extensions := []string{}
	if exts, exists := tagsByName[awsrds.TagExtensions]; exists {
		extensions = strings.Split(exts, ":")
	}

	tags := b.dbTags(RDSInstanceTags{
		Action:         "Restored",
		ServiceID:      serviceID,
		PlanID:         planID,
		OrganizationID: organizationID,
		SpaceID:        spaceID,
		Extensions:     extensions,
	})

	rdsTags := awsrds.BuilRDSTags(tags)
	b.dbInstance.AddTagsToResource(aws.StringValue(updatedDBInstance.DBInstanceArn), rdsTags)
	// AddTagsToResource error intentionally ignored - it's logged inside the method

	return true, nil
}

func (b *RDSBroker) rebootInstance(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperarionTriggered bool, err error) {
	rebootDBInstanceInput := &rds.RebootDBInstanceInput{
		DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
	}

	err = b.dbInstance.Reboot(rebootDBInstanceInput)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *RDSBroker) openSQLEngineForDBInstance(instanceID string, dbName string, dbInstance *rds.DBInstance) (sqlengine.SQLEngine, error) {
	dbAddress := awsrds.GetDBAddress(dbInstance.Endpoint)
	dbPort := awsrds.GetDBPort(dbInstance.Endpoint)
	masterUsername := aws.StringValue(dbInstance.MasterUsername)

	var engine string
	if dbInstance.Engine != nil {
		engine = *dbInstance.Engine
	}
	sqlEngine, err := b.sqlProvider.GetSQLEngine(engine)
	if err != nil {
		b.logger.Error(fmt.Sprintf("Could not determine SQL Engine %s of instance %v", engine, dbName), err)
		return nil, err
	}

	err = sqlEngine.Open(dbAddress, dbPort, dbName, masterUsername, b.generateMasterPassword(instanceID))
	if err != nil {
		sqlEngine.Close()
		return nil, err
	}

	return sqlEngine, err
}

func (b *RDSBroker) changeUserPassword(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperarionTriggered bool, err error) {
	dbName := b.dbNameFromDBInstance(instanceID, dbInstance)
	sqlEngine, err := b.openSQLEngineForDBInstance(instanceID, dbName, dbInstance)
	if err != nil {
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

	dbInstances, err := b.dbInstance.DescribeByTag("Broker Name", b.brokerName)
	if err != nil {
		b.logger.Error("Could not obtain the list of instances", err)
		return
	}

	b.logger.Debug(fmt.Sprintf("Found %v RDS instances managed by the broker", len(dbInstances)))

	for _, dbInstance := range dbInstances {
		dbInstanceIdentifier := aws.StringValue(dbInstance.DBInstanceIdentifier)
		b.logger.Debug(fmt.Sprintf("Checking credentials for instance %v", dbInstanceIdentifier))
		serviceInstanceID := b.dbInstanceIdentifierToServiceInstanceID(dbInstanceIdentifier)
		masterPassword := b.generateMasterPassword(serviceInstanceID)

		// Hey, this is wrong:
		dbName := b.dbNameFromDBInstance(dbInstanceIdentifier, dbInstance)

		sqlEngine, err := b.openSQLEngineForDBInstance(serviceInstanceID, dbName, dbInstance)
		if sqlEngine != nil {
			sqlEngine.Close()
		}
		if err != nil {
			if err == sqlengine.LoginFailedError {
				b.logger.Info(
					"Login failed when connecting to DB. Will attempt to reset the password.",
					lager.Data{"engine": sqlEngine, "endpoint": dbInstance.Endpoint})
				changePasswordInput := &rds.ModifyDBInstanceInput{
					DBInstanceIdentifier: dbInstance.DBInstanceIdentifier,
					MasterUserPassword:   aws.String(masterPassword),
				}
				_, err = b.dbInstance.Modify(changePasswordInput)
				if err != nil {
					b.logger.Error(fmt.Sprintf("Could not reset the master password of instance %v", dbInstanceIdentifier), err)
				}
			} else {
				b.logger.Error(fmt.Sprintf("Unknown error when connecting to DB"), err, lager.Data{"id": dbInstanceIdentifier, "endpoint": dbInstance.Endpoint})
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

func (b *RDSBroker) createDBInstance(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) (*rds.CreateDBInstanceInput, error) {
	skipFinalSnapshot := false
	if provisionParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *provisionParameters.SkipFinalSnapshot
	} else if servicePlan.RDSProperties.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *servicePlan.RDSProperties.SkipFinalSnapshot
	}

	tags := RDSInstanceTags{
		Action:            "Created",
		ServiceID:         details.ServiceID,
		PlanID:            details.PlanID,
		OrganizationID:    details.OrganizationGUID,
		SpaceID:           details.SpaceGUID,
		SkipFinalSnapshot: strconv.FormatBool(skipFinalSnapshot),
		Extensions:        provisionParameters.Extensions,
	}

	parameterGroupName, err := b.parameterGroupsSelector.SelectParameterGroup(servicePlan, provisionParameters)
	if err != nil {
		return nil, err
	}

	return &rds.CreateDBInstanceInput{
		DBInstanceIdentifier:       aws.String(b.dbInstanceIdentifier(instanceID)),
		DBName:                     aws.String(b.dbName(instanceID)),
		MasterUsername:             aws.String(b.generateMasterUsername()),
		MasterUserPassword:         aws.String(b.generateMasterPassword(instanceID)),
		DBInstanceClass:            servicePlan.RDSProperties.DBInstanceClass,
		Engine:                     servicePlan.RDSProperties.Engine,
		AutoMinorVersionUpgrade:    servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		AvailabilityZone:           servicePlan.RDSProperties.AvailabilityZone,
		CopyTagsToSnapshot:         servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:       aws.String(parameterGroupName),
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
		PreferredBackupWindow:      servicePlan.RDSProperties.PreferredBackupWindow,
		StorageEncrypted:           servicePlan.RDSProperties.StorageEncrypted,
		StorageType:                servicePlan.RDSProperties.StorageType,
		VpcSecurityGroupIds:        servicePlan.RDSProperties.VpcSecurityGroupIds,
		Tags:                       awsrds.BuilRDSTags(b.dbTags(tags)),
	}, nil
}

func (b *RDSBroker) restoreDBInstanceInput(instanceID, snapshotIdentifier string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) (*rds.RestoreDBInstanceFromDBSnapshotInput, error) {
	skipFinalSnapshot := false
	if provisionParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *provisionParameters.SkipFinalSnapshot
	} else if servicePlan.RDSProperties.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *servicePlan.RDSProperties.SkipFinalSnapshot
	}
	skipFinalSnapshotStr := strconv.FormatBool(skipFinalSnapshot)

	parameterGroupName, err := b.parameterGroupsSelector.SelectParameterGroup(servicePlan, provisionParameters)
	if err != nil {
		return nil, err
	}

	//"Restored", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshotStr, snapshotIdentifier, provisionParameters.Extensions
	tags := RDSInstanceTags{
		Action:                   "Restored",
		ServiceID:                details.ServiceID,
		PlanID:                   details.PlanID,
		OrganizationID:           details.OrganizationGUID,
		SpaceID:                  details.SpaceGUID,
		SkipFinalSnapshot:        skipFinalSnapshotStr,
		OriginSnapshotIdentifier: snapshotIdentifier,
		Extensions:               provisionParameters.Extensions,
	}

	return &rds.RestoreDBInstanceFromDBSnapshotInput{
		DBSnapshotIdentifier:    aws.String(snapshotIdentifier),
		DBInstanceIdentifier:    aws.String(b.dbInstanceIdentifier(instanceID)),
		DBInstanceClass:         servicePlan.RDSProperties.DBInstanceClass,
		Engine:                  servicePlan.RDSProperties.Engine,
		AutoMinorVersionUpgrade: servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		AvailabilityZone:        servicePlan.RDSProperties.AvailabilityZone,
		CopyTagsToSnapshot:      servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:    aws.String(parameterGroupName),
		DBSubnetGroupName:       servicePlan.RDSProperties.DBSubnetGroupName,
		OptionGroupName:         servicePlan.RDSProperties.OptionGroupName,
		PubliclyAccessible:      servicePlan.RDSProperties.PubliclyAccessible,
		Iops:                    servicePlan.RDSProperties.Iops,
		LicenseModel:            servicePlan.RDSProperties.LicenseModel,
		MultiAZ:                 servicePlan.RDSProperties.MultiAZ,
		Port:                    servicePlan.RDSProperties.Port,
		StorageType:             servicePlan.RDSProperties.StorageType,
		Tags:                    awsrds.BuilRDSTags(b.dbTags(tags)),
	}, nil
}

func (b *RDSBroker) newModifyDBInstanceInput(instanceID string, servicePlan ServicePlan, parameterGroupName *string) *rds.ModifyDBInstanceInput {
	modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier:       aws.String(b.dbInstanceIdentifier(instanceID)),
		DBInstanceClass:            servicePlan.RDSProperties.DBInstanceClass,
		AutoMinorVersionUpgrade:    servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		CopyTagsToSnapshot:         servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:       parameterGroupName,
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
		instanceIDLogKey:  instanceID,
		servicePlanLogKey: servicePlan,
		dbInstanceLogKey:  modifyDBInstanceInput,
	})

	return modifyDBInstanceInput

}

func (b *RDSBroker) dbTags(instanceTags RDSInstanceTags) map[string]string {
	tags := make(map[string]string)

	tags["Owner"] = "Cloud Foundry"

	tags[awsrds.TagBrokerName] = b.brokerName

	tags[instanceTags.Action+" by"] = "AWS RDS Service Broker"

	tags[instanceTags.Action+" at"] = time.Now().Format(time.RFC822Z)

	if instanceTags.ServiceID != "" {
		tags[awsrds.TagServiceID] = instanceTags.ServiceID
	}

	if instanceTags.PlanID != "" {
		tags[awsrds.TagPlanID] = instanceTags.PlanID
	}

	if instanceTags.OrganizationID != "" {
		tags[awsrds.TagOrganizationID] = instanceTags.OrganizationID
	}

	if instanceTags.SpaceID != "" {
		tags[awsrds.TagSpaceID] = instanceTags.SpaceID
	}

	if instanceTags.SkipFinalSnapshot != "" {
		tags[awsrds.TagSkipFinalSnapshot] = instanceTags.SkipFinalSnapshot
	}

	if instanceTags.OriginSnapshotIdentifier != "" {
		tags[awsrds.TagRestoredFromSnapshot] = instanceTags.OriginSnapshotIdentifier
		for _, state := range restoreStateSequence {
			tags[state] = "true"
		}
	}

	if len(instanceTags.Extensions) > 0 {
		tags[awsrds.TagExtensions] = strings.Join(instanceTags.Extensions, ":")
	}

	return tags
}
