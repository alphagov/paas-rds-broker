package rdsbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pivotal-cf/brokerapi/domain/apiresponses"
	"net/http"
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

const RestoreFromLatestSnapshotBeforeTimeFormat = "2006-01-02 15:04:05"
const RestoreFromPointInTimeBeforeTimeFormat = "2006-01-02 15:04:05"

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
	ErrCannotSkipMajorVersion  = errors.New("cannot skip major Postgres versions. Please upgrade one major version at a time (e.g. 10, to 11, to 12)")
	ErrCannotDowngrade         = errors.New("cannot downgrade major versions")
)

var rdsStatus2State = map[string]brokerapi.LastOperationState{
	"available":                           brokerapi.Succeeded,
	"storage-optimization":                brokerapi.Succeeded,
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
	OriginDatabaseIdentifier string
	OriginPointInTime        string
	Extensions               []string
	ChargeableEntity         string
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

func (b *RDSBroker) Services(ctx context.Context) ([]brokerapi.Service, error) {
	brokerCatalog, err := json.Marshal(b.catalog)
	if err != nil {
		b.logger.Error("marshal-error", err)
		return []brokerapi.Service{}, err
	}

	apiCatalog := CatalogExternal{}
	if err = json.Unmarshal(brokerCatalog, &apiCatalog); err != nil {
		b.logger.Error("unmarshal-error", err)
		return []brokerapi.Service{}, err
	}

	for i := range apiCatalog.Services {
		apiCatalog.Services[i].Bindable = true
	}

	return apiCatalog.Services, nil
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
		decoder := json.NewDecoder(bytes.NewReader(details.RawParameters))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&provisionParameters); err != nil {
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
		provisionParameters.Extensions = mergeExtensions(aws.StringValueSlice(servicePlan.RDSProperties.DefaultExtensions), provisionParameters.Extensions)
		ok, unsupportedExtensions := extensionsAreSupported(servicePlan, provisionParameters.Extensions)
		if !ok {
			return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("%s is not supported", unsupportedExtensions)
		}
	}

	if provisionParameters.RestoreFromLatestSnapshotOf != nil && provisionParameters.RestoreFromPointInTimeOf != nil {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Cannot use both restore_from_latest_snapshot_of and restore_from_point_in_time_of at the same time")
	}

	if provisionParameters.RestoreFromLatestSnapshotOf == nil && provisionParameters.RestoreFromLatestSnapshotBefore != nil {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Parameter restore_from_latest_snapshot_before should be used with restore_from_latest_snapshot_of")
	}

	if provisionParameters.RestoreFromLatestSnapshotOf != nil {
		err := b.restoreFromSnapshot(
			ctx, instanceID, details, asyncAllowed,
			provisionParameters, servicePlan,
		)
		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}

	} else if provisionParameters.RestoreFromPointInTimeOf != nil {
		err := b.restoreFromPointInTime(
			ctx, instanceID, details, asyncAllowed,
			provisionParameters, servicePlan,
		)
		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}

	} else {
		createDBInstance, err := b.newCreateDBInstanceInput(instanceID, servicePlan, provisionParameters, details)
		if err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
		if err := b.dbInstance.Create(createDBInstance); err != nil {
			return brokerapi.ProvisionedServiceSpec{}, err
		}
	}

	return brokerapi.ProvisionedServiceSpec{IsAsync: true}, nil
}

func (b *RDSBroker) checkPermissionsFromTags(
	details brokerapi.ProvisionDetails,
	tagsByName map[string]string,
) error {
	if tagsByName[awsrds.TagSpaceID] != details.SpaceGUID || tagsByName[awsrds.TagOrganizationID] != details.OrganizationGUID {
		return fmt.Errorf("The service instance you are getting a snapshot from is not in the same org or space")
	}
	if tagsByName[awsrds.TagPlanID] != details.PlanID {
		return fmt.Errorf("You must use the same plan as the service instance you are restoring from")
	}

	return nil
}

func (b *RDSBroker) restoreFromPointInTime(
	ctx context.Context,
	instanceID string,
	details brokerapi.ProvisionDetails,
	asyncAllowed bool,
	provisionParameters ProvisionParameters,
	servicePlan ServicePlan,
) error {
	if servicePlan.RDSProperties.Engine != nil && *servicePlan.RDSProperties.Engine != "postgres" {
		return fmt.Errorf("Restore from snapshot not supported for engine '%s'", *servicePlan.RDSProperties.Engine)
	}
	if *provisionParameters.RestoreFromPointInTimeOf == "" {
		return fmt.Errorf("Invalid guid: '%s'", *provisionParameters.RestoreFromPointInTimeOf)
	}

	var restoreTime *time.Time
	if provisionParameters.RestoreFromPointInTimeBefore != nil {
		if *provisionParameters.RestoreFromPointInTimeBefore != "" {
			parsedTime, err := time.ParseInLocation(
				RestoreFromPointInTimeBeforeTimeFormat,
				*provisionParameters.RestoreFromPointInTimeBefore,
				time.UTC,
			)
			if err != nil {
				return fmt.Errorf("Parameter restore_from_point_in_time_before should be a date and a time: %s", err)
			}
			restoreTime = &parsedTime
		}
	}

	restoreFromDBInstanceID := *provisionParameters.RestoreFromPointInTimeOf

	existingInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(restoreFromDBInstanceID))
	if err != nil {
		return fmt.Errorf("Cannot find instance %s", b.dbInstanceIdentifier(restoreFromDBInstanceID))
	}

	dbARN := *(existingInstance.DBInstanceArn)
	tags, err := b.dbInstance.GetResourceTags(dbARN)
	if err != nil {
		return fmt.Errorf("Cannot find instance %s", dbARN)
	}

	tagsByName := awsrds.RDSTagsValues(tags)
	if err := b.checkPermissionsFromTags(details, tagsByName); err != nil {
		return err
	}

	if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
		if extensionsTag != "" {
			existingExts := strings.Split(extensionsTag, ":")
			provisionParameters.Extensions = mergeExtensions(provisionParameters.Extensions, existingExts)
		}
	}

	restoreInput, err := b.restoreDBInstancePointInTimeInput(instanceID, restoreFromDBInstanceID, restoreTime, servicePlan, provisionParameters, details)
	if err != nil {
		return err
	}

	return b.dbInstance.RestoreToPointInTime(restoreInput)
}

func (b *RDSBroker) restoreFromSnapshot(
	ctx context.Context,
	instanceID string,
	details brokerapi.ProvisionDetails,
	asyncAllowed bool,
	provisionParameters ProvisionParameters,
	servicePlan ServicePlan,
) error {
	if *provisionParameters.RestoreFromLatestSnapshotOf == "" {
		return fmt.Errorf("Invalid guid: '%s'", *provisionParameters.RestoreFromLatestSnapshotOf)
	}
	if servicePlan.RDSProperties.Engine != nil && *servicePlan.RDSProperties.Engine != "postgres" {
		return fmt.Errorf("Restore from snapshot not supported for engine '%s'", *servicePlan.RDSProperties.Engine)
	}
	restoreFromDBInstanceID := b.dbInstanceIdentifier(*provisionParameters.RestoreFromLatestSnapshotOf)
	snapshots, err := b.dbInstance.DescribeSnapshots(restoreFromDBInstanceID)
	if err != nil {
		return err
	}

	if provisionParameters.RestoreFromLatestSnapshotBefore != nil {
		if *provisionParameters.RestoreFromLatestSnapshotBefore == "" {
			return fmt.Errorf("Parameter restore_from_latest_snapshot_before must not be empty")
		}

		restoreFromLatestSnapshotBeforeTime, err := time.ParseInLocation(
			RestoreFromLatestSnapshotBeforeTimeFormat,
			*provisionParameters.RestoreFromLatestSnapshotBefore,
			time.UTC,
		)
		if err != nil {
			return fmt.Errorf("Parameter restore_from_latest_snapshot_before should be a date and a time: %s", err)
		}

		prunedSnapshots := make([]*rds.DBSnapshot, 0)
		for _, snapshot := range snapshots {
			if snapshot.SnapshotCreateTime.Before(restoreFromLatestSnapshotBeforeTime) {
				prunedSnapshots = append(prunedSnapshots, snapshot)
			}
		}

		b.logger.Info("pruned-snapshots", lager.Data{
			"instanceIDLogKey":     instanceID,
			"detailsLogKey":        details,
			"allSnapshotsCount":    len(snapshots),
			"prunedSnapshotsCount": len(prunedSnapshots),
		})

		snapshots = prunedSnapshots
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("No snapshots found for guid '%s'", *provisionParameters.RestoreFromLatestSnapshotOf)
	}

	snapshot := snapshots[0]

	b.logger.Info("chose-snapshot", lager.Data{
		"instanceIDLogKey":   instanceID,
		"detailsLogKey":      details,
		"snapshotIdentifier": snapshot.DBSnapshotIdentifier,
	})

	tags, err := b.dbInstance.GetResourceTags(aws.StringValue(snapshot.DBSnapshotArn))
	if err != nil {
		return err
	}

	tagsByName := awsrds.RDSTagsValues(tags)
	if err := b.checkPermissionsFromTags(details, tagsByName); err != nil {
		return err
	}

	snapshotIdentifier := aws.StringValue(snapshot.DBSnapshotIdentifier)

	if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
		if extensionsTag != "" {
			snapshotExts := strings.Split(extensionsTag, ":")
			provisionParameters.Extensions = mergeExtensions(provisionParameters.Extensions, snapshotExts)
		}
	}

	restoreDBInstanceInput, err := b.restoreDBInstanceInput(instanceID, snapshotIdentifier, servicePlan, provisionParameters, details)
	if err != nil {
		return err
	}

	return b.dbInstance.Restore(restoreDBInstanceInput)
}

func (b *RDSBroker) GetBinding(ctx context.Context, first, second string) (brokerapi.GetBindingSpec, error) {
	return brokerapi.GetBindingSpec{}, fmt.Errorf("GetBinding method not implemented")
}

func (b *RDSBroker) GetInstance(ctx context.Context, first string) (brokerapi.GetInstanceDetailsSpec, error) {
	return brokerapi.GetInstanceDetailsSpec{}, fmt.Errorf("GetInstance method not implemented")
}

func (b *RDSBroker) LastBindingOperation(ctx context.Context, first, second string, pollDetails brokerapi.PollDetails) (brokerapi.LastOperation, error) {
	return brokerapi.LastOperation{}, fmt.Errorf("LastBindingOperation method not implemented")
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
		decoder := json.NewDecoder(bytes.NewReader(details.RawParameters))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&updateParameters); err != nil {
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

	if details.PlanID != details.PreviousValues.PlanID {
		if !service.PlanUpdatable {
			return brokerapi.UpdateServiceSpec{}, brokerapi.ErrPlanChangeNotSupported
		}
		err := updateParameters.CheckForCompatibilityWithPlanChange()
		if err != nil {
			return brokerapi.UpdateServiceSpec{}, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	previousServicePlan, ok := b.catalog.FindServicePlan(details.PreviousValues.PlanID)
	if !ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PreviousValues.PlanID)
	}

	isPlanUpgrade, err := servicePlan.IsUpgradeFrom(previousServicePlan)
	if err != nil {
		b.logger.Error("is-service-plan-an-upgrade", err)
		return brokerapi.UpdateServiceSpec{}, err
	}

	oldVersion, err := previousServicePlan.EngineVersion()
	if err != nil {
		return brokerapi.UpdateServiceSpec{}, err
	}
	newVersion, err := servicePlan.EngineVersion()
	if err != nil {
		return brokerapi.UpdateServiceSpec{}, err
	}

	if newVersion.Major() < oldVersion.Major() {
		err := ErrCannotDowngrade
		b.logger.Error("downgrade-attempted", err)
		return brokerapi.UpdateServiceSpec{},
			apiresponses.NewFailureResponse(
				err,
				http.StatusBadRequest,
				"upgrade",
			)
	}

	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" {
		majorVersionDifference := newVersion.Major() - oldVersion.Major()
		if majorVersionDifference > 1 {
			err := ErrCannotSkipMajorVersion
			b.logger.Error("invalid-upgrade-path", err)
			return brokerapi.UpdateServiceSpec{},
				apiresponses.NewFailureResponse(
					err,
					http.StatusBadRequest,
					"upgrade",
				)
		}
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

	previousDbParamGroup := *existingInstance.DBParameterGroups[0].DBParameterGroupName

	newDbParamGroup := previousDbParamGroup

	ok, unsupportedExtension := extensionsAreSupported(servicePlan, mergeExtensions(updateParameters.EnableExtensions, updateParameters.DisableExtensions))
	if !ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("%s is not supported", unsupportedExtension)
	}

	ok, defaultExtension := containsDefaultExtension(servicePlan, updateParameters.DisableExtensions)
	if ok {
		return brokerapi.UpdateServiceSpec{}, fmt.Errorf("%s cannot be disabled", defaultExtension)
	}

	extensions := mergeExtensions(aws.StringValueSlice(servicePlan.RDSProperties.DefaultExtensions), updateParameters.EnableExtensions)

	tags, err := b.dbInstance.GetResourceTags(aws.StringValue(existingInstance.DBInstanceArn))
	if err != nil {
		return brokerapi.UpdateServiceSpec{}, err
	}
	tagsByName := awsrds.RDSTagsValues(tags)

	if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
		if extensionsTag != "" {
			extensions = mergeExtensions(extensions, strings.Split(extensionsTag, ":"))
		}
	}

	extensions = removeExtensions(extensions, updateParameters.DisableExtensions)
	err = b.ensureDropExtensions(instanceID, existingInstance, updateParameters.DisableExtensions)
	if err != nil {
		return brokerapi.UpdateServiceSpec{}, err
	}

	deferReboot := false

	newDbParamGroup, err = b.parameterGroupsSelector.SelectParameterGroup(servicePlan, extensions)
	if err != nil {
		return brokerapi.UpdateServiceSpec{}, err
	}

	if (len(updateParameters.EnableExtensions) > 0 || len(updateParameters.DisableExtensions) > 0) && newDbParamGroup != previousDbParamGroup {
		if updateParameters.Reboot == nil || !*updateParameters.Reboot {
			return brokerapi.UpdateServiceSpec{}, errors.New("The requested extensions require the instance to be manually rebooted. Please re-run update service with reboot set to true")
		}
		// When updating the parameter group, the instance will be in a modifying state
		// for a couple of mins. So we have to defer the reboot to the last operation call.
		deferReboot = true
	}

	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan, updateParameters, newDbParamGroup)

	if updateParameters.UpgradeMinorVersionToLatest != nil && *updateParameters.UpgradeMinorVersionToLatest {
		b.logger.Info("is-minor-version-upgrade")
		if updateParameters.Reboot != nil && *updateParameters.Reboot {
			return brokerapi.UpdateServiceSpec{}, fmt.Errorf(
				"Cannot reboot and upgrade minor version to latest at the same time",
			)
		}

		if details.PlanID != details.PreviousValues.PlanID {
			return brokerapi.UpdateServiceSpec{}, fmt.Errorf(
				"Cannot specify a version and upgrade minor version to latest at the same time",
			)
		}

		availableEngineVersion, err := b.dbInstance.GetLatestMinorVersion(
			*existingInstance.Engine,
			*existingInstance.EngineVersion,
		)
		b.logger.Info("selected-minor-version", lager.Data{"version": availableEngineVersion})

		if err != nil {
			return brokerapi.UpdateServiceSpec{}, err
		}

		if availableEngineVersion != nil {
			modifyDBInstanceInput.EngineVersion = availableEngineVersion
		}
	}

	if isPlanUpgrade {
		b.logger.Info("is-a-version-upgrade")
		b.logger.Info("find-exact-upgrade-version")
		currentVersion := *existingInstance.EngineVersion
		targetVersion, err := b.dbInstance.GetFullValidTargetVersion(
			*servicePlan.RDSProperties.Engine,
			currentVersion,
			*servicePlan.RDSProperties.EngineVersion,
		)

		if err != nil {
			b.logger.Error("find-exact-upgrade-version", err)
			return brokerapi.UpdateServiceSpec{}, err
		}

		b.logger.Info("selected-upgrade-version", lager.Data{"version": targetVersion})
		modifyDBInstanceInput.EngineVersion = aws.String(targetVersion)
	}

	updatedDBInstance, err := b.dbInstance.Modify(modifyDBInstanceInput)
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.UpdateServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.UpdateServiceSpec{}, err
	}

	instanceTags := RDSInstanceTags{
		Action:           "Updated",
		ServiceID:        details.ServiceID,
		PlanID:           details.PlanID,
		Extensions:       extensions,
		ChargeableEntity: instanceID,
	}

	if updateParameters.SkipFinalSnapshot != nil {
		instanceTags.SkipFinalSnapshot = strconv.FormatBool(*updateParameters.SkipFinalSnapshot)
	}

	builtTags := awsrds.BuilRDSTags(b.dbTags(instanceTags))
	b.dbInstance.AddTagsToResource(aws.StringValue(updatedDBInstance.DBInstanceArn), builtTags)

	if updateParameters.Reboot != nil && *updateParameters.Reboot && !deferReboot {
		rebootDBInstanceInput := &rds.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
			ForceFailover:        updateParameters.ForceFailover,
		}

		err := b.dbInstance.Reboot(rebootDBInstanceInput)
		if err != nil {
			return brokerapi.UpdateServiceSpec{}, err
		}
		return brokerapi.UpdateServiceSpec{IsAsync: true}, nil
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
	asyncAllowed bool,
) (brokerapi.Binding, error) {
	b.logger.Debug("bind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	bindingResponse := brokerapi.Binding{}

	bindParameters := BindParameters{}
	if b.allowUserBindParameters && len(details.RawParameters) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(details.RawParameters))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&bindParameters); err != nil {
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

	if aws.StringValue(dbInstance.Engine) != "postgres" && bindParameters.ReadOnly {
		return bindingResponse, fmt.Errorf("Read only bindings are only supported for postgres")
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

	dbUsername, dbPassword, err := sqlEngine.CreateUser(bindingID, dbName, bindParameters.ReadOnly)
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
	asyncAllowed bool,
) (brokerapi.UnbindSpec, error) {
	b.logger.Debug("unbind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	_, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return brokerapi.UnbindSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return brokerapi.UnbindSpec{}, brokerapi.ErrInstanceDoesNotExist
		}
		return brokerapi.UnbindSpec{}, err
	}

	dbName := b.dbNameFromDBInstance(instanceID, dbInstance)
	sqlEngine, err := b.openSQLEngineForDBInstance(instanceID, dbName, dbInstance)
	if err != nil {
		return brokerapi.UnbindSpec{}, err
	}
	defer sqlEngine.Close()

	if err = sqlEngine.DropUser(bindingID); err != nil {
		return brokerapi.UnbindSpec{}, err
	}

	return brokerapi.UnbindSpec{}, nil
}

func (b *RDSBroker) LastOperation(
	ctx context.Context,
	instanceID string,
	pollDetails brokerapi.PollDetails,
) (brokerapi.LastOperation, error) {
	b.logger.Debug("last-operation", lager.Data{
		instanceIDLogKey: instanceID,
	})

	var lastOperationResponse brokerapi.LastOperation

	defer func() {
		b.logger.Debug("last-operation.done", lager.Data{
			instanceIDLogKey:            instanceID,
			lastOperationResponseLogKey: lastOperationResponse,
		})
	}()

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

	lastOperationResponse = brokerapi.LastOperation{
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
			return lastOperationResponse, nil
		}

		asyncOperationTriggered, err := b.PostRestoreTasks(instanceID, dbInstance, tagsByName)
		if err != nil {
			return brokerapi.LastOperation{State: brokerapi.Failed}, err
		}
		if asyncOperationTriggered {
			lastOperationResponse = brokerapi.LastOperation{
				State:       brokerapi.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' has pending post restore modifications", b.dbInstanceIdentifier(instanceID)),
			}
			return lastOperationResponse, nil
		}

		asyncOperationTriggered, err = b.RebootIfRequired(instanceID, dbInstance)
		if err != nil {
			return brokerapi.LastOperation{State: brokerapi.Failed}, err
		}
		if asyncOperationTriggered {
			lastOperationResponse = brokerapi.LastOperation{
				State:       brokerapi.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' is rebooting", b.dbInstanceIdentifier(instanceID)),
			}
			return lastOperationResponse, nil
		}

		err = b.ensureCreateExtensions(instanceID, dbInstance, tagsByName)
		if err != nil {
			return brokerapi.LastOperation{State: brokerapi.Failed}, err
		}
	}

	return lastOperationResponse, nil
}

func searchExtension(slice []string, element string) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}

	return false
}

func mergeExtensions(l1 []string, l2 []string) []string {
	var result []string
	for _, e := range l1 {
		result = append(result, e)
	}

	for _, e := range l2 {
		if !searchExtension(result, e) {
			result = append(result, e)
		}
	}

	return result
}

func removeExtensions(extensions []string, exclude []string) []string {
	var result []string
	for _, e := range extensions {
		if !searchExtension(exclude, e) {
			result = append(result, e)
		}
	}

	return result
}

func extensionsAreSupported(plan ServicePlan, extensions []string) (bool, string) {
	supported := aws.StringValueSlice(plan.RDSProperties.AllowedExtensions)
	for _, e := range extensions {
		if !searchExtension(supported, e) {
			return false, e
		}
	}
	return true, ""
}

func containsDefaultExtension(plan ServicePlan, extensions []string) (bool, string) {
	defaultExtensions := aws.StringValueSlice(plan.RDSProperties.DefaultExtensions)
	for _, e := range extensions {
		if searchExtension(defaultExtensions, e) {
			return true, e
		}
	}
	return false, ""
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

func (b *RDSBroker) ensureDropExtensions(instanceID string, dbInstance *rds.DBInstance, extensions []string) error {
	b.logger.Debug("ensure-drop-extensions", lager.Data{
		instanceIDLogKey: instanceID,
	})

	if aws.StringValue(dbInstance.Engine) == "postgres" && len(extensions) > 0 {
		dbName := b.dbNameFromDBInstance(instanceID, dbInstance)
		sqlEngine, err := b.openSQLEngineForDBInstance(instanceID, dbName, dbInstance)
		if err != nil {
			return err
		}
		defer sqlEngine.Close()

		if err = sqlEngine.DropExtensions(extensions); err != nil {
			return err
		}
	}

	return nil
}

func (b *RDSBroker) updateDBSettings(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperationTriggered bool, err error) {
	serviceID := tagsByName[awsrds.TagServiceID]
	planID := tagsByName[awsrds.TagPlanID]
	organizationID := tagsByName[awsrds.TagOrganizationID]
	spaceID := tagsByName[awsrds.TagSpaceID]

	servicePlan, ok := b.catalog.FindServicePlan(planID)
	if !ok {
		return false, fmt.Errorf("Service Plan '%s' not found", tagsByName[awsrds.TagPlanID])
	}

	existingParameterGroup := aws.StringValue(dbInstance.DBParameterGroups[0].DBParameterGroupName)

	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan, UpdateParameters{}, existingParameterGroup)
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
		Action:           "Restored",
		ServiceID:        serviceID,
		PlanID:           planID,
		OrganizationID:   organizationID,
		SpaceID:          spaceID,
		Extensions:       extensions,
		ChargeableEntity: instanceID,
	})

	rdsTags := awsrds.BuilRDSTags(tags)
	b.dbInstance.AddTagsToResource(aws.StringValue(updatedDBInstance.DBInstanceArn), rdsTags)
	// AddTagsToResource error intentionally ignored - it's logged inside the method

	return true, nil
}

func (b *RDSBroker) rebootInstance(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperationTriggered bool, err error) {
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

func (b *RDSBroker) changeUserPassword(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperationTriggered bool, err error) {
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

func (b *RDSBroker) PostRestoreTasks(instanceID string, dbInstance *rds.DBInstance, tagsByName map[string]string) (asyncOperationTriggered bool, err error) {
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

func (b *RDSBroker) RebootIfRequired(instanceID string, dbInstance *rds.DBInstance) (asyncOperationTriggered bool, err error) {
	if aws.StringValue(dbInstance.DBParameterGroups[0].ParameterApplyStatus) == "applying" {
		return true, nil
	}

	if aws.StringValue(dbInstance.DBParameterGroups[0].ParameterApplyStatus) == "pending-reboot" {
		rebootDBInstanceInput := &rds.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
		}

		err := b.dbInstance.Reboot(rebootDBInstanceInput)
		if err != nil {
			return false, err
		}
		return true, nil
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

func (b *RDSBroker) newCreateDBInstanceInput(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) (*rds.CreateDBInstanceInput, error) {
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
		ChargeableEntity:  instanceID,
	}

	parameterGroupName, err := b.parameterGroupsSelector.SelectParameterGroup(servicePlan, provisionParameters.Extensions)
	if err != nil {
		return nil, err
	}

	createDBInstanceInput := &rds.CreateDBInstanceInput{
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
	}
	if provisionParameters.PreferredBackupWindow != "" {
		createDBInstanceInput.PreferredBackupWindow = aws.String(provisionParameters.PreferredBackupWindow)
	}
	if provisionParameters.PreferredMaintenanceWindow != "" {
		createDBInstanceInput.PreferredMaintenanceWindow = aws.String(provisionParameters.PreferredMaintenanceWindow)
	}
	return createDBInstanceInput, nil
}

func (b *RDSBroker) restoreDBInstanceInput(instanceID, snapshotIdentifier string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) (*rds.RestoreDBInstanceFromDBSnapshotInput, error) {
	skipFinalSnapshot := false
	if provisionParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *provisionParameters.SkipFinalSnapshot
	} else if servicePlan.RDSProperties.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *servicePlan.RDSProperties.SkipFinalSnapshot
	}
	skipFinalSnapshotStr := strconv.FormatBool(skipFinalSnapshot)

	parameterGroupName, err := b.parameterGroupsSelector.SelectParameterGroup(servicePlan, provisionParameters.Extensions)
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
		ChargeableEntity:         instanceID,
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

func (b *RDSBroker) restoreDBInstancePointInTimeInput(instanceID, originDBIdentifier string, originTime *time.Time, servicePlan ServicePlan, provisionParameters ProvisionParameters, details brokerapi.ProvisionDetails) (*rds.RestoreDBInstanceToPointInTimeInput, error) {
	skipFinalSnapshot := false
	if provisionParameters.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *provisionParameters.SkipFinalSnapshot
	} else if servicePlan.RDSProperties.SkipFinalSnapshot != nil {
		skipFinalSnapshot = *servicePlan.RDSProperties.SkipFinalSnapshot
	}
	skipFinalSnapshotStr := strconv.FormatBool(skipFinalSnapshot)

	parameterGroupName, err := b.parameterGroupsSelector.SelectParameterGroup(servicePlan, provisionParameters.Extensions)
	if err != nil {
		return nil, err
	}

	tags := RDSInstanceTags{
		Action:                   "Restored",
		ServiceID:                details.ServiceID,
		PlanID:                   details.PlanID,
		OrganizationID:           details.OrganizationGUID,
		SpaceID:                  details.SpaceGUID,
		SkipFinalSnapshot:        skipFinalSnapshotStr,
		OriginDatabaseIdentifier: b.dbInstanceIdentifier(originDBIdentifier),
		Extensions:               provisionParameters.Extensions,
		ChargeableEntity:         instanceID,
	}

	if originTime != nil {
		tags.OriginPointInTime = originTime.Format(time.RFC3339)
	}

	input := &rds.RestoreDBInstanceToPointInTimeInput{
		SourceDBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(originDBIdentifier)),
		TargetDBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
		RestoreTime:                originTime,
		DBInstanceClass:            servicePlan.RDSProperties.DBInstanceClass,
		Engine:                     servicePlan.RDSProperties.Engine,
		AutoMinorVersionUpgrade:    servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		AvailabilityZone:           servicePlan.RDSProperties.AvailabilityZone,
		CopyTagsToSnapshot:         servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:       aws.String(parameterGroupName),
		DBSubnetGroupName:          servicePlan.RDSProperties.DBSubnetGroupName,
		OptionGroupName:            servicePlan.RDSProperties.OptionGroupName,
		PubliclyAccessible:         servicePlan.RDSProperties.PubliclyAccessible,
		Iops:                       servicePlan.RDSProperties.Iops,
		LicenseModel:               servicePlan.RDSProperties.LicenseModel,
		MultiAZ:                    servicePlan.RDSProperties.MultiAZ,
		Port:                       servicePlan.RDSProperties.Port,
		StorageType:                servicePlan.RDSProperties.StorageType,
		Tags:                       awsrds.BuilRDSTags(b.dbTags(tags)),
	}

	if originTime != nil {
		input.RestoreTime = originTime
	} else {
		input.UseLatestRestorableTime = aws.Bool(true)
	}

	return input, nil
}

func (b *RDSBroker) newModifyDBInstanceInput(instanceID string, servicePlan ServicePlan, updateParameters UpdateParameters, parameterGroupName string) *rds.ModifyDBInstanceInput {
	modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier:       aws.String(b.dbInstanceIdentifier(instanceID)),
		DBInstanceClass:            servicePlan.RDSProperties.DBInstanceClass,
		AutoMinorVersionUpgrade:    servicePlan.RDSProperties.AutoMinorVersionUpgrade,
		CopyTagsToSnapshot:         servicePlan.RDSProperties.CopyTagsToSnapshot,
		DBParameterGroupName:       aws.String(parameterGroupName),
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
		ApplyImmediately:           aws.Bool(!updateParameters.ApplyAtMaintenanceWindow),
	}
	if updateParameters.PreferredBackupWindow != "" {
		modifyDBInstanceInput.PreferredBackupWindow = aws.String(updateParameters.PreferredBackupWindow)
	}
	if updateParameters.PreferredMaintenanceWindow != "" {
		modifyDBInstanceInput.PreferredMaintenanceWindow = aws.String(updateParameters.PreferredMaintenanceWindow)
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

	tags["chargeable_entity"] = instanceTags.ChargeableEntity

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

	if instanceTags.OriginDatabaseIdentifier != "" {
		tags[awsrds.TagOriginDatabase] = instanceTags.OriginDatabaseIdentifier
	}

	if instanceTags.OriginPointInTime != "" {
		tags[awsrds.TagOriginPointInTime] = instanceTags.OriginPointInTime
	}

	if instanceTags.OriginSnapshotIdentifier != "" || instanceTags.OriginDatabaseIdentifier != "" {
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
