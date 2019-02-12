package rdsbroker

import (
	"code.cloudfoundry.org/lager"
	"fmt"
	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"sort"
	"strings"
)

//go:generate counterfeiter -o fakes/fake_parameter_group_selector.go . ParameterGroupSelector
type ParameterGroupSelector interface {
	SelectParameterGroup(servicePlan ServicePlan, parameters ProvisionParameters) (string, error)
}

type ParameterGroupSource struct {
	config      Config
	rdsInstance awsrds.RDSInstance
	logger      lager.Logger
}

func NewParameterGroupSource(config Config, rdsInstance awsrds.RDSInstance, logger lager.Logger) *ParameterGroupSource {
	return &ParameterGroupSource{config, rdsInstance, logger}
}

func (pgs *ParameterGroupSource) SelectParameterGroup(servicePlan ServicePlan, parameters ProvisionParameters) (string, error) {
	pgs.logger.Debug("selecting a parameter group", lager.Data{
		servicePlanLogKey: servicePlan,
		extensionsLogKey:  parameters.Extensions,
	})

	groupName := composeGroupName(pgs.config, servicePlan, parameters)
	pgs.logger.Info(fmt.Sprintf("database should be created with parameter group '%s'", groupName))
	_, err := pgs.rdsInstance.GetParameterGroup(groupName)

	if err != nil {
		if !isParameterGroupNotFoundError(err) {
			return "", err
		} else {
			err := pgs.createParameterGroup(groupName, servicePlan)
			if err != nil {
				return "", err
			}

			err = pgs.setParameterGroupProperties(groupName, servicePlan, parameters)
			if err != nil {
				return "", err
			}

			return groupName, nil
		}
	}

	pgs.logger.Info(fmt.Sprintf("parameter group '%s' already existed", groupName))
	return groupName, nil
}

func (pgs *ParameterGroupSource) createParameterGroup(name string, servicePlan ServicePlan) error {
	pgs.logger.Debug("creating a parameter group", lager.Data{
		"groupName": name,
	})
	family := aws.StringValue(servicePlan.RDSProperties.Engine) + aws.StringValue(servicePlan.RDSProperties.EngineVersion)

	return pgs.rdsInstance.CreateParameterGroup(&rds.CreateDBParameterGroupInput{
		DBParameterGroupFamily: aws.String(family),
		DBParameterGroupName:   aws.String(name),
		Description:            aws.String(name),
		Tags:                   nil,
	})
}

func (pgs *ParameterGroupSource) setParameterGroupProperties(name string, servicePlan ServicePlan, provisionParameters ProvisionParameters) error {
	dbParams := []*rds.Parameter{}
	dbParams = append(dbParams, rdsParameter("rds.force_ssl", "1", "pending-reboot"))
	dbParams = append(dbParams, rdsParameter("rds.log_retention_period", "10080", "immediate"))

	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" {
		preloadLibs := filterExtensionsNeedingPreloads(servicePlan, provisionParameters.Extensions)
		libsCSV := strings.Join(preloadLibs, ",")
		dbParams = append(dbParams, rdsParameter("shared_preload_libraries", libsCSV, "pending-reboot"))
	}

	pgs.logger.Debug("modifying a parameter group", lager.Data{
		"groupName":  name,
		"parameters": dbParams,
	})

	return pgs.rdsInstance.ModifyParameterGroup(&rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(name),
		Parameters:           dbParams,
	})
}

func composeGroupName(config Config, servicePlan ServicePlan, provisionParameters ProvisionParameters) string {

	normalisedExtensions := []string{}
	normalisedEngine := normaliseIdentifier(aws.StringValue(servicePlan.RDSProperties.Engine))
	normalisedVersion := normaliseIdentifier(aws.StringValue(servicePlan.RDSProperties.EngineVersion))

	relevantExtensions := filterExtensionsNeedingPreloads(servicePlan, provisionParameters.Extensions)
	for _, ext := range relevantExtensions {
		normalisedExtensions = append(normalisedExtensions, normaliseIdentifier(ext))
	}

	// Sort extensions alphabetically
	// so that user input doesn't cause
	// more unique parameter group names
	// than necessary
	sort.Strings(normalisedExtensions)

	identifier := fmt.Sprintf(
		"%s-%s%s-%s",
		config.DBPrefix,
		normalisedEngine,
		normalisedVersion,
		config.BrokerName,
	)

	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" && len(normalisedExtensions) > 0 {
		identifier = fmt.Sprintf("%s-%s", identifier, strings.Join(normalisedExtensions, "-"))
	}

	return identifier
}

func filterExtensionsNeedingPreloads(servicePlan ServicePlan, requestedExtensions []string) []string {
	normalisedEngine := normaliseIdentifier(aws.StringValue(servicePlan.RDSProperties.Engine))
	normalisedVersion := normaliseIdentifier(aws.StringValue(servicePlan.RDSProperties.EngineVersion))

	supportedExtensions := []DBExtension{}
	if exts, ok := SupportedPreloadExtensions[normalisedEngine+normalisedVersion]; ok {
		supportedExtensions = exts
	}

	relevantExtensions := []string{}
	for _, ext := range requestedExtensions {
		for _, supported := range supportedExtensions {
			if (supported.RequiresPreloadLibrary) && ext == supported.Name {
				relevantExtensions = append(relevantExtensions, ext)
				break
			}
		}
	}

	return relevantExtensions
}

func isParameterGroupNotFoundError(err error) bool {
	return strings.HasPrefix(err.Error(), rds.ErrCodeDBParameterGroupNotFoundFault)
}

func normaliseIdentifier(value string) string {
	bannedChars := []string{".", "_", "-"}
	out := value
	for _, char := range bannedChars {
		out = strings.Replace(out, char, "", -1)
	}

	return out
}

func rdsParameter(paramName string, paramValue string, applyMethod string) *rds.Parameter {
	return &rds.Parameter{
		ParameterName:  aws.String(paramName),
		ParameterValue: aws.String(paramValue),
		ApplyMethod:    aws.String(applyMethod),
	}
}
