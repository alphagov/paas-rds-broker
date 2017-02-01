package rdsbroker

import (
	"fmt"
	"strings"

	"github.com/pivotal-cf/brokerapi"
)

const minAllocatedStorage = 5
const maxAllocatedStorage = 6144

type Catalog struct {
	Services []Service `json:"services,omitempty"`
}

type CatalogExternal struct {
	Services []brokerapi.Service `json:"services"`
}

type Service struct {
	ID              string                            `json:"id"`
	Name            string                            `json:"name"`
	Description     string                            `json:"description"`
	Bindable        bool                              `json:"bindable"`
	Tags            []string                          `json:"tags,omitempty"`
	PlanUpdatable   bool                              `json:"plan_updateable"`
	Plans           []ServicePlan                     `json:"plans"`
	Requires        []brokerapi.RequiredPermission    `json:"requires,omitempty"`
	Metadata        *brokerapi.ServiceMetadata        `json:"metadata,omitempty"`
	DashboardClient *brokerapi.ServiceDashboardClient `json:"dashboard_client,omitempty"`
}

type ServicePlan struct {
	ID            string                         `json:"id"`
	Name          string                         `json:"name"`
	Description   string                         `json:"description"`
	Free          *bool                          `json:"free,omitempty"`
	Metadata      *brokerapi.ServicePlanMetadata `json:"metadata,omitempty"`
	RDSProperties RDSProperties                  `json:"rds_properties,omitempty"`
}

type RDSProperties struct {
	DBInstanceClass            string   `json:"db_instance_class"`
	Engine                     string   `json:"engine"`
	EngineVersion              string   `json:"engine_version"`
	AllocatedStorage           int64    `json:"allocated_storage"`
	AutoMinorVersionUpgrade    bool     `json:"auto_minor_version_upgrade,omitempty"`
	AvailabilityZone           string   `json:"availability_zone,omitempty"`
	BackupRetentionPeriod      int64    `json:"backup_retention_period,omitempty"`
	CharacterSetName           string   `json:"character_set_name,omitempty"`
	DBParameterGroupName       string   `json:"db_parameter_group_name,omitempty"`
	DBSecurityGroups           []string `json:"db_security_groups,omitempty"`
	DBSubnetGroupName          string   `json:"db_subnet_group_name,omitempty"`
	LicenseModel               string   `json:"license_model,omitempty"`
	MultiAZ                    bool     `json:"multi_az,omitempty"`
	OptionGroupName            string   `json:"option_group_name,omitempty"`
	Port                       int64    `json:"port,omitempty"`
	PreferredBackupWindow      string   `json:"preferred_backup_window,omitempty"`
	PreferredMaintenanceWindow string   `json:"preferred_maintenance_window,omitempty"`
	PubliclyAccessible         bool     `json:"publicly_accessible,omitempty"`
	StorageEncrypted           bool     `json:"storage_encrypted,omitempty"`
	KmsKeyID                   string   `json:"kms_key_id,omitempty"`
	StorageType                string   `json:"storage_type,omitempty"`
	Iops                       int64    `json:"iops,omitempty"`
	VpcSecurityGroupIds        []string `json:"vpc_security_group_ids,omitempty"`
	CopyTagsToSnapshot         bool     `json:"copy_tags_to_snapshot,omitempty"`
	SkipFinalSnapshot          bool     `json:"skip_final_snapshot,omitempty"`
}

func (c Catalog) Validate() error {
	for _, service := range c.Services {
		if err := service.Validate(); err != nil {
			return fmt.Errorf("Validating Services configuration: %s", err)
		}
	}

	return nil
}

func (c Catalog) FindService(serviceID string) (service Service, found bool) {
	for _, service := range c.Services {
		if service.ID == serviceID {
			return service, true
		}
	}

	return service, false
}

func (c Catalog) FindServicePlan(planID string) (plan ServicePlan, found bool) {
	for _, service := range c.Services {
		for _, plan := range service.Plans {
			if plan.ID == planID {
				return plan, true
			}
		}
	}

	return plan, false
}

func (s Service) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("Must provide a non-empty ID (%+v)", s)
	}

	if s.Name == "" {
		return fmt.Errorf("Must provide a non-empty Name (%+v)", s)
	}

	if s.Description == "" {
		return fmt.Errorf("Must provide a non-empty Description (%+v)", s)
	}

	for _, servicePlan := range s.Plans {
		if err := servicePlan.Validate(); err != nil {
			return fmt.Errorf("Validating Plans configuration: %s", err)
		}
	}

	return nil
}

func (sp ServicePlan) Validate() error {
	if sp.ID == "" {
		return fmt.Errorf("Must provide a non-empty ID (%+v)", sp)
	}

	if sp.Name == "" {
		return fmt.Errorf("Must provide a non-empty Name (%+v)", sp)
	}

	if sp.Description == "" {
		return fmt.Errorf("Must provide a non-empty Description (%+v)", sp)
	}

	if err := sp.RDSProperties.Validate(); err != nil {
		return fmt.Errorf("Validating RDS Properties configuration: %s", err)
	}

	return nil
}

func (rp RDSProperties) Validate() error {
	if rp.DBInstanceClass == "" {
		return fmt.Errorf("Must provide a non-empty DBInstanceClass (%+v)", rp)
	}

	if rp.Engine == "" {
		return fmt.Errorf("Must provide a non-empty Engine (%+v)", rp)
	}

	switch strings.ToLower(rp.Engine) {
	case "mariadb":
	case "mysql":
	case "postgres":
	default:
		return fmt.Errorf("This broker does not support RDS engine '%s' (%+v)", rp.Engine, rp)
	}

	return nil
}
