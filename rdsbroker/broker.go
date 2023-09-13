package rdsbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pivotal-cf/brokerapi/v8/domain/apiresponses"

	"code.cloudfoundry.org/lager/v3"
	"github.com/pivotal-cf/brokerapi/v8/domain"

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

const warningOverAllocatedStorage = "OverAllocatedStorage"

const disagreementEngine = "Engine"
const disagreementAllocatedStorage = "AllocatedStorage"
const disagreementMultiAZ = "MultiAZ"
const disagreementDBInstanceClass = "DBInstanceClass"

var (
	ErrEncryptionNotUpdateable = errors.New("instance can not be updated to a plan with different encryption settings")
	ErrCannotSkipMajorVersion  = errors.New("cannot skip major Postgres versions. Please upgrade one major version at a time (e.g. 10, to 11, to 12)")
	ErrCannotDowngradeVersion  = errors.New("cannot downgrade major versions")
	ErrCannotDowngradeStorage  = errors.New("cannot downgrade storage")
)

var rdsStatus2State = map[string]domain.LastOperationState{
	"available":                           domain.Succeeded,
	"storage-optimization":                domain.Succeeded,
	"backing-up":                          domain.InProgress,
	"creating":                            domain.InProgress,
	"deleting":                            domain.InProgress,
	"maintenance":                         domain.InProgress,
	"modifying":                           domain.InProgress,
	"rebooting":                           domain.InProgress,
	"renaming":                            domain.InProgress,
	"resetting-master-credentials":        domain.InProgress,
	"upgrading":                           domain.InProgress,
	"configuring-enhanced-monitoring":     domain.InProgress,
	"starting":                            domain.InProgress,
	"stopping":                            domain.InProgress,
	"stopped":                             domain.InProgress,
	"storage-full":                        domain.InProgress,
	"failed":                              domain.Failed,
	"incompatible-credentials":            domain.Failed,
	"incompatible-network":                domain.Failed,
	"incompatible-option-group":           domain.Failed,
	"incompatible-parameters":             domain.Failed,
	"incompatible-restore":                domain.Failed,
	"restore-error":                       domain.Failed,
	"inaccessible-encryption-credentials": domain.Failed,
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

func (b *RDSBroker) Services(ctx context.Context) ([]domain.Service, error) {
	brokerCatalog, err := json.Marshal(b.catalog)
	if err != nil {
		b.logger.Error("marshal-error", err)
		return []domain.Service{}, err
	}

	apiCatalog := CatalogExternal{}
	if err = json.Unmarshal(brokerCatalog, &apiCatalog); err != nil {
		b.logger.Error("unmarshal-error", err)
		return []domain.Service{}, err
	}

	for i := range apiCatalog.Services {
		apiCatalog.Services[i].Bindable = true
		apiCatalog.Services[i].InstancesRetrievable = true
	}

	return apiCatalog.Services, nil
}

func (b *RDSBroker) Provision(
	ctx context.Context,
	instanceID string,
	details domain.ProvisionDetails,
	asyncAllowed bool,
) (domain.ProvisionedServiceSpec, error) {
	b.logger.Debug("provision", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	if !asyncAllowed {
		return domain.ProvisionedServiceSpec{}, apiresponses.ErrAsyncRequired
	}

	provisionParameters := ProvisionParameters{}
	if b.allowUserProvisionParameters && len(details.RawParameters) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(details.RawParameters))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&provisionParameters); err != nil {
			return domain.ProvisionedServiceSpec{}, err
		}
		if err := provisionParameters.Validate(); err != nil {
			return domain.ProvisionedServiceSpec{}, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return domain.ProvisionedServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" {
		provisionParameters.Extensions = mergeExtensions(aws.StringValueSlice(servicePlan.RDSProperties.DefaultExtensions), provisionParameters.Extensions)
		ok, unsupportedExtensions := extensionsAreSupported(servicePlan, provisionParameters.Extensions)
		if !ok {
			return domain.ProvisionedServiceSpec{}, fmt.Errorf("%s is not supported", unsupportedExtensions)
		}
	}

	if provisionParameters.RestoreFromLatestSnapshotOf != nil && provisionParameters.RestoreFromPointInTimeOf != nil {
		return domain.ProvisionedServiceSpec{}, fmt.Errorf("Cannot use both restore_from_latest_snapshot_of and restore_from_point_in_time_of at the same time")
	}

	if provisionParameters.RestoreFromLatestSnapshotOf == nil && provisionParameters.RestoreFromLatestSnapshotBefore != nil {
		return domain.ProvisionedServiceSpec{}, fmt.Errorf("Parameter restore_from_latest_snapshot_before should be used with restore_from_latest_snapshot_of")
	}

	if provisionParameters.RestoreFromPointInTimeOf == nil && provisionParameters.RestoreFromPointInTimeBefore != nil {
		return domain.ProvisionedServiceSpec{}, fmt.Errorf("Parameter restore_from_point_in_time_before should be used with restore_from_point_in_time_of")
	}

	if provisionParameters.RestoreFromLatestSnapshotOf != nil {
		err := b.restoreFromSnapshot(
			ctx, instanceID, details, asyncAllowed,
			provisionParameters, servicePlan,
		)
		if err != nil {
			return domain.ProvisionedServiceSpec{}, err
		}

	} else if provisionParameters.RestoreFromPointInTimeOf != nil {
		err := b.restoreFromPointInTime(
			ctx, instanceID, details, asyncAllowed,
			provisionParameters, servicePlan,
		)
		if err != nil {
			return domain.ProvisionedServiceSpec{}, err
		}

	} else {
		createDBInstance, err := b.newCreateDBInstanceInput(instanceID, servicePlan, provisionParameters, details)
		if err != nil {
			return domain.ProvisionedServiceSpec{}, err
		}
		if err := b.dbInstance.Create(createDBInstance); err != nil {
			return domain.ProvisionedServiceSpec{}, err
		}
	}

	return domain.ProvisionedServiceSpec{IsAsync: true}, nil
}

func (b *RDSBroker) checkPermissionsFromTags(
	details domain.ProvisionDetails,
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
	details domain.ProvisionDetails,
	asyncAllowed bool,
	provisionParameters ProvisionParameters,
	servicePlan ServicePlan,
) error {
	if engine := servicePlan.RDSProperties.Engine; engine != nil {
		if *engine != "postgres" && *engine != "mysql" {
			return fmt.Errorf("Restore from point in time not supported for engine '%s'", *engine)
		}
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
			existingExts := unpackExtensions(extensionsTag)
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
	details domain.ProvisionDetails,
	asyncAllowed bool,
	provisionParameters ProvisionParameters,
	servicePlan ServicePlan,
) error {
	if *provisionParameters.RestoreFromLatestSnapshotOf == "" {
		return fmt.Errorf("Invalid guid: '%s'", *provisionParameters.RestoreFromLatestSnapshotOf)
	}
	if engine := servicePlan.RDSProperties.Engine; engine != nil {
		if *engine != "postgres" && *engine != "mysql" {
			return fmt.Errorf("Restore from snapshot not supported for engine '%s'", *engine)
		}
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
			instanceIDLogKey:       instanceID,
			detailsLogKey:          details,
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
		instanceIDLogKey:     instanceID,
		detailsLogKey:        details,
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

	if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
		if extensionsTag != "" {
			snapshotExts := unpackExtensions(extensionsTag)
			provisionParameters.Extensions = mergeExtensions(provisionParameters.Extensions, snapshotExts)
		}
	}

	restoreDBInstanceInput, err := b.restoreDBInstanceInput(instanceID, snapshot, servicePlan, provisionParameters, details)
	if err != nil {
		return err
	}

	return b.dbInstance.Restore(restoreDBInstanceInput)
}

func (b *RDSBroker) GetBinding(ctx context.Context, instanceID, bindingID string, details domain.FetchBindingDetails) (domain.GetBindingSpec, error) {
	return domain.GetBindingSpec{}, fmt.Errorf("GetBinding method not implemented")
}

func (b *RDSBroker) GetInstance(
	ctx context.Context,
	instanceID string,
	details domain.FetchInstanceDetails,
) (domain.GetInstanceDetailsSpec, error) {
	b.logger.Debug("get-instance", lager.Data{
		instanceIDLogKey: instanceID,
	})

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		b.logger.Error("describe-instance", err)
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return domain.GetInstanceDetailsSpec{}, apiresponses.ErrInstanceDoesNotExist
		}
		return domain.GetInstanceDetailsSpec{}, err
	}

	tags, err := b.dbInstance.GetResourceTags(aws.StringValue(dbInstance.DBInstanceArn))
	if err != nil {
		b.logger.Error("get-instance-tags", err)
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return domain.GetInstanceDetailsSpec{}, apiresponses.ErrInstanceDoesNotExist
		}
		return domain.GetInstanceDetailsSpec{}, err
	}
	tagsByName := awsrds.RDSTagsValues(tags)

	extensions := []string{}
	if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
		extensions = unpackExtensions(extensionsTag)
	}

	var ok bool
	planID := details.PlanID
	if planID == "" {
		// supplying the plan id as a GET parameter doesn't appear to be
		// a mandatory part of the OSB spec but we prefer it if it's
		// present as it doesn't have the same (small) possibility of
		// being incorrect that the aws tag has, which we fall back to
		// here
		planID, ok = tagsByName[awsrds.TagPlanID]
		if !ok {
			err = fmt.Errorf("Can't find plan id for this service instance")
			b.logger.Error("cant-find-plan-id", err)
			return domain.GetInstanceDetailsSpec{}, err
		}
	}
	servicePlan, ok := b.catalog.FindServicePlan(planID)
	if !ok {
		return domain.GetInstanceDetailsSpec{}, fmt.Errorf("Service Plan '%s' not found", planID)
	}

	skipFinalSnapshot, err := resolveSkipFinalSnapshot(servicePlan, tagsByName[awsrds.TagSkipFinalSnapshot])
	if err != nil {
		b.logger.Error("resolve-skip-final-snapshot", err)
		return domain.GetInstanceDetailsSpec{}, err
	}

	instanceParams := map[string]interface{}{
		"backup_retention_period":      dbInstance.BackupRetentionPeriod,
		"extensions":                   extensions,
		"preferred_backup_window":      dbInstance.PreferredBackupWindow,
		"preferred_maintenance_window": dbInstance.PreferredMaintenanceWindow,
		"skip_final_snapshot":          skipFinalSnapshot,
	}

	if tagsByName[awsrds.TagOriginDatabase] != "" {
		if tagsByName[awsrds.TagOriginPointInTime] != "" {
			instanceParams["restored_from_point_in_time_of"] = b.dbInstanceIdentifierToServiceInstanceID(tagsByName[awsrds.TagOriginDatabase])
			instanceParams["restored_from_point_in_time_before"] = tagsByName[awsrds.TagOriginPointInTime]
		}
		if tagsByName[awsrds.TagRestoredFromSnapshot] != "" {
			// "restored_from_latest_snapshot_of" would be misleading as
			// we don't know what value of restore_from_point_in_time_before
			// was used at provisioning
			instanceParams["restored_from_snapshot_of"] = b.dbInstanceIdentifierToServiceInstanceID(tagsByName[awsrds.TagOriginDatabase])
		}
	}

	return domain.GetInstanceDetailsSpec{
		Parameters: instanceParams,
	}, nil
}

func (b *RDSBroker) LastBindingOperation(ctx context.Context, first, second string, pollDetails domain.PollDetails) (domain.LastOperation, error) {
	return domain.LastOperation{}, fmt.Errorf("LastBindingOperation method not implemented")
}

func (b *RDSBroker) Update(
	ctx context.Context,
	instanceID string,
	details domain.UpdateDetails,
	asyncAllowed bool,
) (domain.UpdateServiceSpec, error) {
	b.logger.Debug("update", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	b.logger.Info("update", lager.Data{instanceIDLogKey: instanceID, detailsLogKey: details})

	if !asyncAllowed {
		return domain.UpdateServiceSpec{}, apiresponses.ErrAsyncRequired
	}

	updateParameters := UpdateParameters{}
	if b.allowUserUpdateParameters && len(details.RawParameters) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(details.RawParameters))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&updateParameters); err != nil {
			return domain.UpdateServiceSpec{}, err
		}
		if err := updateParameters.Validate(); err != nil {
			return domain.UpdateServiceSpec{}, err
		}
		b.logger.Debug("update-parsed-params", lager.Data{updateParametersLogKey: updateParameters})
	}

	service, ok := b.catalog.FindService(details.ServiceID)
	if !ok {
		return domain.UpdateServiceSpec{}, fmt.Errorf("Service '%s' not found", details.ServiceID)
	}

	if details.PlanID != details.PreviousValues.PlanID {
		if !service.PlanUpdatable {
			return domain.UpdateServiceSpec{}, apiresponses.ErrPlanChangeNotSupported
		}
		err := updateParameters.CheckForCompatibilityWithPlanChange()
		if err != nil {
			return domain.UpdateServiceSpec{}, err
		}
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return domain.UpdateServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	previousServicePlan, ok := b.catalog.FindServicePlan(details.PreviousValues.PlanID)
	if !ok {
		return domain.UpdateServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PreviousValues.PlanID)
	}

	isPlanUpgrade, err := servicePlan.IsUpgradeFrom(previousServicePlan)
	if err != nil {
		b.logger.Error("is-service-plan-an-upgrade", err)
		return domain.UpdateServiceSpec{}, err
	}

	oldVersion, err := previousServicePlan.EngineVersion()
	if err != nil {
		return domain.UpdateServiceSpec{}, err
	}
	newVersion, err := servicePlan.EngineVersion()
	if err != nil {
		return domain.UpdateServiceSpec{}, err
	}

	if newVersion.Major() < oldVersion.Major() {
		err := ErrCannotDowngradeVersion
		b.logger.Error("version-downgrade-attempted", err)
		return domain.UpdateServiceSpec{},
			apiresponses.NewFailureResponse(
				err,
				http.StatusBadRequest,
				"upgrade",
			)
	}

	if *servicePlan.RDSProperties.AllocatedStorage < *previousServicePlan.RDSProperties.AllocatedStorage {
		err := ErrCannotDowngradeStorage
		b.logger.Error("storage-downgrade-attempted", err)
		return domain.UpdateServiceSpec{},
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
			return domain.UpdateServiceSpec{},
				apiresponses.NewFailureResponse(
					err,
					http.StatusBadRequest,
					"upgrade",
				)
		}
	}

	if !reflect.DeepEqual(servicePlan.RDSProperties.StorageEncrypted, previousServicePlan.RDSProperties.StorageEncrypted) {
		return domain.UpdateServiceSpec{}, ErrEncryptionNotUpdateable
	}

	if !reflect.DeepEqual(servicePlan.RDSProperties.KmsKeyID, previousServicePlan.RDSProperties.KmsKeyID) {
		return domain.UpdateServiceSpec{}, ErrEncryptionNotUpdateable
	}

	existingInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))

	if err != nil {
		return domain.UpdateServiceSpec{}, fmt.Errorf("cannot find instance %s", b.dbInstanceIdentifier(instanceID))
	}

	if aws.StringValue(existingInstance.DBInstanceStatus) == "storage-full" {
		return domain.UpdateServiceSpec{},
			fmt.Errorf("Cannot update instance %s because it is in state \"storage-full\". You will need to contact support to resolve this issue.",
				b.dbInstanceIdentifier(instanceID))
	}

	previousDbParamGroup := *existingInstance.DBParameterGroups[0].DBParameterGroupName

	newDbParamGroup := previousDbParamGroup

	ok, unsupportedExtension := extensionsAreSupported(servicePlan, mergeExtensions(updateParameters.EnableExtensions, updateParameters.DisableExtensions))
	if !ok {
		return domain.UpdateServiceSpec{}, fmt.Errorf("%s is not supported", unsupportedExtension)
	}

	ok, defaultExtension := containsDefaultExtension(servicePlan, updateParameters.DisableExtensions)
	if ok {
		return domain.UpdateServiceSpec{}, fmt.Errorf("%s cannot be disabled", defaultExtension)
	}

	extensions := mergeExtensions(aws.StringValueSlice(servicePlan.RDSProperties.DefaultExtensions), updateParameters.EnableExtensions)

	tags, err := b.dbInstance.GetResourceTags(aws.StringValue(existingInstance.DBInstanceArn))
	if err != nil {
		return domain.UpdateServiceSpec{}, err
	}
	tagsByName := awsrds.RDSTagsValues(tags)

	if extensionsTag, ok := tagsByName[awsrds.TagExtensions]; ok {
		if extensionsTag != "" {
			extensions = mergeExtensions(extensions, unpackExtensions(extensionsTag))
		}
	}

	extensions = removeExtensions(extensions, updateParameters.DisableExtensions)
	err = b.ensureDropExtensions(instanceID, existingInstance, updateParameters.DisableExtensions)
	if err != nil {
		return domain.UpdateServiceSpec{}, err
	}

	deferReboot := false

	newDbParamGroup, err = b.parameterGroupsSelector.SelectParameterGroup(servicePlan, extensions)
	if err != nil {
		return domain.UpdateServiceSpec{}, err
	}

	if (len(updateParameters.EnableExtensions) > 0 || len(updateParameters.DisableExtensions) > 0) && newDbParamGroup != previousDbParamGroup {
		if updateParameters.Reboot == nil || !*updateParameters.Reboot {
			return domain.UpdateServiceSpec{}, errors.New("The requested extensions require the instance to be manually rebooted. Please re-run update service with reboot set to true")
		}
		// When updating the parameter group, the instance will be in a modifying state
		// for a couple of mins. So we have to defer the reboot to the last operation call.
		deferReboot = true
	}

	modifyDBInstanceInput := b.newModifyDBInstanceInput(instanceID, servicePlan, updateParameters, newDbParamGroup)

	if updateParameters.UpgradeMinorVersionToLatest != nil && *updateParameters.UpgradeMinorVersionToLatest {
		b.logger.Info("is-minor-version-upgrade")
		if updateParameters.Reboot != nil && *updateParameters.Reboot {
			return domain.UpdateServiceSpec{}, fmt.Errorf(
				"Cannot reboot and upgrade minor version to latest at the same time",
			)
		}

		if details.PlanID != details.PreviousValues.PlanID {
			return domain.UpdateServiceSpec{}, fmt.Errorf(
				"Cannot specify a version and upgrade minor version to latest at the same time",
			)
		}

		availableEngineVersion, err := b.dbInstance.GetLatestMinorVersion(
			*existingInstance.Engine,
			*existingInstance.EngineVersion,
		)
		b.logger.Info("selected-minor-version", lager.Data{"version": availableEngineVersion})

		if err != nil {
			return domain.UpdateServiceSpec{}, err
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
			return domain.UpdateServiceSpec{}, err
		}
		if targetVersion != "" {
			b.logger.Info("selected-upgrade-version", lager.Data{"version": targetVersion})
			modifyDBInstanceInput.EngineVersion = aws.String(targetVersion)
		}
	}

	updatedDBInstance, err := b.dbInstance.Modify(modifyDBInstanceInput)
	if err != nil {
		if awsRdsErr, ok := err.(awsrds.Error); ok {
			switch code := awsRdsErr.Code(); code {
			case awsrds.ErrCodeDBInstanceDoesNotExist:
				return domain.UpdateServiceSpec{},
					apiresponses.ErrInstanceDoesNotExist
			case awsrds.ErrCodeInvalidParameterCombination:
				return domain.UpdateServiceSpec{},
					apiresponses.NewFailureResponse(
						err,
						http.StatusUnprocessableEntity,
						"upgrade",
					)
			}
		}
		return domain.UpdateServiceSpec{}, err
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

	builtTags := awsrds.BuildRDSTags(b.dbTags(instanceTags))
	b.dbInstance.AddTagsToResource(aws.StringValue(updatedDBInstance.DBInstanceArn), builtTags)

	if updateParameters.Reboot != nil && *updateParameters.Reboot && !deferReboot {
		rebootDBInstanceInput := &rds.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String(b.dbInstanceIdentifier(instanceID)),
			ForceFailover:        updateParameters.ForceFailover,
		}

		err := b.dbInstance.Reboot(rebootDBInstanceInput)
		if err != nil {
			return domain.UpdateServiceSpec{}, err
		}
		return domain.UpdateServiceSpec{IsAsync: true}, nil
	}

	return domain.UpdateServiceSpec{IsAsync: true}, nil
}

// determine whether we actually want to skip final snapshot given
// servicePlan and tagValue
func resolveSkipFinalSnapshot(servicePlan ServicePlan, tagValue string) (bool, error) {
	var err error
	skipDBInstanceFinalSnapshot := servicePlan.RDSProperties.SkipFinalSnapshot == nil || *servicePlan.RDSProperties.SkipFinalSnapshot

	if tagValue != "" {
		skipDBInstanceFinalSnapshot, err = strconv.ParseBool(tagValue)
		if err != nil {
			return false, err
		}
	}

	return skipDBInstanceFinalSnapshot, nil
}

func (b *RDSBroker) Deprovision(
	ctx context.Context,
	instanceID string,
	details domain.DeprovisionDetails,
	asyncAllowed bool,
) (domain.DeprovisionServiceSpec, error) {
	b.logger.Debug("deprovision", lager.Data{
		instanceIDLogKey:   instanceID,
		detailsLogKey:      details,
		asyncAllowedLogKey: asyncAllowed,
	})

	if !asyncAllowed {
		return domain.DeprovisionServiceSpec{}, apiresponses.ErrAsyncRequired
	}

	servicePlan, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return domain.DeprovisionServiceSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	skipFinalSnapshot, err := b.dbInstance.GetTag(b.dbInstanceIdentifier(instanceID), awsrds.TagSkipFinalSnapshot)
	if err != nil {
		return domain.DeprovisionServiceSpec{}, err
	}

	skipDBInstanceFinalSnapshot, err := resolveSkipFinalSnapshot(servicePlan, skipFinalSnapshot)
	if err != nil {
		return domain.DeprovisionServiceSpec{}, err
	}

	if err := b.dbInstance.Delete(b.dbInstanceIdentifier(instanceID), skipDBInstanceFinalSnapshot); err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return domain.DeprovisionServiceSpec{}, apiresponses.ErrInstanceDoesNotExist
		}
		return domain.DeprovisionServiceSpec{}, err
	}

	return domain.DeprovisionServiceSpec{IsAsync: true}, nil
}

func (b *RDSBroker) Bind(
	ctx context.Context,
	instanceID, bindingID string,
	details domain.BindDetails,
	asyncAllowed bool,
) (domain.Binding, error) {
	b.logger.Debug("bind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	bindingResponse := domain.Binding{}

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
			return bindingResponse, apiresponses.ErrInstanceDoesNotExist
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
	details domain.UnbindDetails,
	asyncAllowed bool,
) (domain.UnbindSpec, error) {
	b.logger.Debug("unbind", lager.Data{
		instanceIDLogKey: instanceID,
		bindingIDLogKey:  bindingID,
		detailsLogKey:    details,
	})

	_, ok := b.catalog.FindServicePlan(details.PlanID)
	if !ok {
		return domain.UnbindSpec{}, fmt.Errorf("Service Plan '%s' not found", details.PlanID)
	}

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			return domain.UnbindSpec{}, apiresponses.ErrInstanceDoesNotExist
		}
		return domain.UnbindSpec{}, err
	}

	dbName := b.dbNameFromDBInstance(instanceID, dbInstance)
	sqlEngine, err := b.openSQLEngineForDBInstance(instanceID, dbName, dbInstance)
	if err != nil {
		return domain.UnbindSpec{}, err
	}
	defer sqlEngine.Close()

	if err = sqlEngine.DropUser(bindingID); err != nil {
		return domain.UnbindSpec{}, err
	}

	return domain.UnbindSpec{}, nil
}

func (b *RDSBroker) LastOperation(
	ctx context.Context,
	instanceID string,
	pollDetails domain.PollDetails,
) (domain.LastOperation, error) {
	b.logger.Debug("last-operation", lager.Data{
		instanceIDLogKey: instanceID,
	})

	var lastOperationResponse domain.LastOperation

	defer func() {
		b.logger.Debug("last-operation.done", lager.Data{
			instanceIDLogKey:            instanceID,
			lastOperationResponseLogKey: lastOperationResponse,
		})
	}()

	dbInstance, err := b.dbInstance.Describe(b.dbInstanceIdentifier(instanceID))
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			err = apiresponses.ErrInstanceDoesNotExist
		}
		return domain.LastOperation{State: domain.Failed}, err
	}

	tags, err := b.dbInstance.GetResourceTags(
		aws.StringValue(dbInstance.DBInstanceArn),
	)
	if err != nil {
		if err == awsrds.ErrDBInstanceDoesNotExist {
			err = apiresponses.ErrInstanceDoesNotExist
		}
		return domain.LastOperation{State: domain.Failed}, err
	}

	tagsByName := awsrds.RDSTagsValues(tags)

	status := aws.StringValue(dbInstance.DBInstanceStatus)
	state, ok := rdsStatus2State[status]
	if !ok {
		state = domain.InProgress
	}

	lastOperationResponse = domain.LastOperation{
		State:       state,
		Description: fmt.Sprintf("DB Instance '%s' status is '%s'", b.dbInstanceIdentifier(instanceID), status),
	}

	if lastOperationResponse.State == domain.Succeeded {
		hasPendingModifications := false
		if dbInstance.PendingModifiedValues != nil {
			emptyPendingModifiedValues := rds.PendingModifiedValues{}
			if !reflect.DeepEqual(*dbInstance.PendingModifiedValues, emptyPendingModifiedValues) {
				hasPendingModifications = true
			}
		}
		if hasPendingModifications {
			lastOperationResponse = domain.LastOperation{
				State:       domain.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' has pending modifications", b.dbInstanceIdentifier(instanceID)),
			}
			return lastOperationResponse, nil
		}

		awsTagsPlanID, _ := tagsByName[awsrds.TagPlanID]
		if pollDetails.PlanID != awsTagsPlanID {
			// this was presumably a plan change
			awsTagsPlan, ok := b.catalog.FindServicePlan(awsTagsPlanID)
			if !ok {
				return domain.LastOperation{State: domain.Failed}, fmt.Errorf(
					"Service Plan '%s' in aws tag '%s' not found",
					awsTagsPlanID,
					awsrds.TagPlanID,
				)
			}
			awsTagsPlanDisagreements, awsTagsPlanWarnings, err := b.compareDBDescriptionWithPlan(
				dbInstance,
				awsTagsPlan,
			)
			if err != nil {
				return domain.LastOperation{State: domain.Failed}, err
			}

			if len(awsTagsPlanWarnings) != 0 {
				b.logger.Info("aws-tags-plan-properties-mismatch-warning", lager.Data{
					instanceIDLogKey: instanceID,
					"awsTagsPlanID":  awsTagsPlanID,
					"warnings":       awsTagsPlanWarnings,
				})
			}

			// if all has gone well, the current state of the instance should
			// match the new plan
			if len(awsTagsPlanDisagreements) != 0 {
				b.logger.Info("aws-tags-plan-properties-mismatch", lager.Data{
					instanceIDLogKey: instanceID,
					"awsTagsPlanID":  awsTagsPlanID,
					"disagreements":  awsTagsPlanDisagreements,
				})
				currentPlan, ok := b.catalog.FindServicePlan(pollDetails.PlanID)
				if !ok {
					return domain.LastOperation{State: domain.Failed}, fmt.Errorf("Service Plan '%s' provided in request not found", pollDetails.PlanID)
				}
				currentPlanDisagreements, currentPlanWarnings, err := b.compareDBDescriptionWithPlan(
					dbInstance,
					currentPlan,
				)
				if err != nil {
					return domain.LastOperation{State: domain.Failed}, err
				}

				if len(awsTagsPlanWarnings) != 0 {
					b.logger.Info("current-plan-properties-mismatch-warning", lager.Data{
						instanceIDLogKey:  instanceID,
						servicePlanLogKey: awsTagsPlanID,
						"warnings":        currentPlanWarnings,
					})
				}

				if len(currentPlanDisagreements) == 0 {
					// we can tell the cloud controller the operation has failed
					// and simply roll back the plan id in the aws tags
					b.logger.Info("rolling-back-failed-plan-change", lager.Data{
						instanceIDLogKey:   instanceID,
						servicePlanLogKey:  pollDetails.PlanID,
						"awsTagsPlanID":    awsTagsPlanID,
						"rdsEngineVersion": *dbInstance.EngineVersion,
					})
					tagsByName[awsrds.TagPlanID] = pollDetails.PlanID
					b.dbInstance.AddTagsToResource(
						aws.StringValue(dbInstance.DBInstanceArn),
						awsrds.BuildRDSTags(tagsByName),
					)
					lastOperationResponse = domain.LastOperation{
						State:       domain.Failed,
						Description: "Plan upgrade failed. Refer to database logs for more information.",
					}
					return lastOperationResponse, nil
				}

				// the current state of the instance matches neither plan, so
				// we can't safely leave it or roll it back
				b.logger.Info("current-plan-properties-mismatch", lager.Data{
					instanceIDLogKey:  instanceID,
					servicePlanLogKey: pollDetails.PlanID,
					"disagreements":   currentPlanDisagreements,
				})
				lastOperationResponse = domain.LastOperation{
					State:       domain.Failed,
					Description: "Operation failed and will need manual intervention to resolve. Please contact support.",
				}
				return lastOperationResponse, nil
			}
		}

		asyncOperationTriggered, err := b.PostRestoreTasks(instanceID, dbInstance, tagsByName)
		if err != nil {
			return domain.LastOperation{State: domain.Failed}, err
		}
		if asyncOperationTriggered {
			lastOperationResponse = domain.LastOperation{
				State:       domain.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' has pending post restore modifications", b.dbInstanceIdentifier(instanceID)),
			}
			return lastOperationResponse, nil
		}

		asyncOperationTriggered, err = b.RebootIfRequired(instanceID, dbInstance)
		if err != nil {
			return domain.LastOperation{State: domain.Failed}, err
		}
		if asyncOperationTriggered {
			lastOperationResponse = domain.LastOperation{
				State:       domain.InProgress,
				Description: fmt.Sprintf("DB Instance '%s' is rebooting", b.dbInstanceIdentifier(instanceID)),
			}
			return lastOperationResponse, nil
		}

		err = b.ensureCreateExtensions(instanceID, dbInstance, tagsByName)
		if err != nil {
			return domain.LastOperation{State: domain.Failed}, err
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
			postgresExtensionsString := unpackExtensions(extensions)

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

// pack array of extensions to their tag-stored format
func packExtensions(unpackedExtensions []string) string {
	return strings.Join(unpackedExtensions, ":")
}

// unpack array of extensions from their tag-stored format
func unpackExtensions(packedExtensions string) []string {
	return strings.Split(packedExtensions, ":")
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
			return false, apiresponses.ErrInstanceDoesNotExist
		}
		return false, err
	}

	extensions := []string{}
	if exts, exists := tagsByName[awsrds.TagExtensions]; exists {
		extensions = unpackExtensions(exts)
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

	rdsTags := awsrds.BuildRDSTags(tags)
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

	dbInstances, err := b.dbInstance.DescribeByTag(
		"Broker Name",
		b.brokerName,
		awsrds.DescribeUseCachedOption,
	)
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

func (b *RDSBroker) newCreateDBInstanceInput(instanceID string, servicePlan ServicePlan, provisionParameters ProvisionParameters, details domain.ProvisionDetails) (*rds.CreateDBInstanceInput, error) {
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
		Tags:                       awsrds.BuildRDSTags(b.dbTags(tags)),
	}
	if provisionParameters.PreferredBackupWindow != "" {
		createDBInstanceInput.PreferredBackupWindow = aws.String(provisionParameters.PreferredBackupWindow)
	}
	if provisionParameters.PreferredMaintenanceWindow != "" {
		createDBInstanceInput.PreferredMaintenanceWindow = aws.String(provisionParameters.PreferredMaintenanceWindow)
	}
	return createDBInstanceInput, nil
}

func (b *RDSBroker) restoreDBInstanceInput(instanceID string, snapshot *rds.DBSnapshot, servicePlan ServicePlan, provisionParameters ProvisionParameters, details domain.ProvisionDetails) (*rds.RestoreDBInstanceFromDBSnapshotInput, error) {
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

	//"Restored", details.ServiceID, details.PlanID, details.OrganizationGUID, details.SpaceGUID, skipFinalSnapshotStr, snapshot.DBSnapshotIdentifier, provisionParameters.Extensions
	tags := RDSInstanceTags{
		Action:                   "Restored",
		ServiceID:                details.ServiceID,
		PlanID:                   details.PlanID,
		OrganizationID:           details.OrganizationGUID,
		SpaceID:                  details.SpaceGUID,
		SkipFinalSnapshot:        skipFinalSnapshotStr,
		OriginSnapshotIdentifier: aws.StringValue(snapshot.DBSnapshotIdentifier),
		OriginDatabaseIdentifier: aws.StringValue(snapshot.DBInstanceIdentifier),
		Extensions:               provisionParameters.Extensions,
		ChargeableEntity:         instanceID,
	}

	return &rds.RestoreDBInstanceFromDBSnapshotInput{
		DBSnapshotIdentifier:    snapshot.DBSnapshotIdentifier,
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
		Tags:                    awsrds.BuildRDSTags(b.dbTags(tags)),
	}, nil
}

func (b *RDSBroker) restoreDBInstancePointInTimeInput(instanceID, originDBIdentifier string, originTime *time.Time, servicePlan ServicePlan, provisionParameters ProvisionParameters, details domain.ProvisionDetails) (*rds.RestoreDBInstanceToPointInTimeInput, error) {
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
		Tags:                       awsrds.BuildRDSTags(b.dbTags(tags)),
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

// compares only the most important properties of the dbInstance with the
// expected RDSProperties in servicePlan
func (b *RDSBroker) compareDBDescriptionWithPlan(dbInstance *rds.DBInstance, servicePlan ServicePlan) ([]string, []string, error) {
	disagreements := []string{}
	warnings := []string{}

	planEngineVersion, err := servicePlan.EngineVersion()
	if err != nil {
		return nil, nil, err
	}
	rdsEngineVersion, err := semver.NewVersion(*dbInstance.EngineVersion)
	if err != nil {
		return nil, nil, err
	}

	if planEngineVersion.Major() != rdsEngineVersion.Major() {
		disagreements = append(disagreements, disagreementEngine)
	}

	if *servicePlan.RDSProperties.AllocatedStorage < *dbInstance.AllocatedStorage {
		warnings = append(warnings, warningOverAllocatedStorage)
	}

	if *servicePlan.RDSProperties.AllocatedStorage > *dbInstance.AllocatedStorage {
		disagreements = append(disagreements, disagreementAllocatedStorage)
	}

	if *servicePlan.RDSProperties.DBInstanceClass != *dbInstance.DBInstanceClass {
		disagreements = append(disagreements, disagreementDBInstanceClass)
	}

	if servicePlan.RDSProperties.MultiAZ != nil && *servicePlan.RDSProperties.MultiAZ != *dbInstance.MultiAZ {
		disagreements = append(disagreements, disagreementMultiAZ)
	}

	return disagreements, warnings, nil
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
		tags[awsrds.TagExtensions] = packExtensions(instanceTags.Extensions)
	}

	return tags
}
