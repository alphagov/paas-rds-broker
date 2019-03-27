package rdsbroker

import (
	"fmt"
	"sort"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
)

//go:generate counterfeiter -o fakes/fake_parameter_group_selector.go . ParameterGroupSelector
type ParameterGroupSelector interface {
	SelectParameterGroup(servicePlan ServicePlan, extensions []string) (string, error)
}

type ParameterGroupSource struct {
	config                     Config
	rdsInstance                awsrds.RDSInstance
	logger                     lager.Logger
	supportedPreloadExtensions map[string][]DBExtension
}

func NewParameterGroupSource(config Config, rdsInstance awsrds.RDSInstance, supportedPreloadExtensions map[string][]DBExtension, logger lager.Logger) *ParameterGroupSource {
	return &ParameterGroupSource{config, rdsInstance, logger, supportedPreloadExtensions}
}

func (pgs *ParameterGroupSource) SelectParameterGroup(servicePlan ServicePlan, extensions []string) (string, error) {
	pgs.logger.Debug("selecting a parameter group", lager.Data{
		servicePlanLogKey: servicePlan,
		extensionsLogKey:  extensions,
	})

	groupName := composeGroupName(pgs.config, servicePlan, extensions, pgs.supportedPreloadExtensions)
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

			err = pgs.setParameterGroupProperties(groupName, servicePlan, extensions)
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

	return pgs.rdsInstance.CreateParameterGroup(&rds.CreateDBParameterGroupInput{
		DBParameterGroupFamily: servicePlan.RDSProperties.EngineFamily,
		DBParameterGroupName:   aws.String(name),
		Description:            aws.String(name),
	})
}

func (pgs *ParameterGroupSource) setParameterGroupProperties(name string, servicePlan ServicePlan, extensions []string) error {
	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" {
		dbParams := []*rds.Parameter{}
		dbParams = append(dbParams, rdsParameter("rds.force_ssl", "1", "pending-reboot"))
		dbParams = append(dbParams, rdsParameter("rds.log_retention_period", "10080", "immediate"))

		preloadLibs := filterExtensionsNeedingPreloads(servicePlan, extensions, pgs.supportedPreloadExtensions)

		if len(preloadLibs) > 0 {
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

	return nil
}

func composeGroupName(config Config, servicePlan ServicePlan, extensions []string, supportedPreloadExtensions map[string][]DBExtension) string {

	normalisedFamily := normaliseIdentifier(aws.StringValue(servicePlan.RDSProperties.EngineFamily))
	normalisedExtensions := []string{}
	relevantExtensions := filterExtensionsNeedingPreloads(servicePlan, extensions, supportedPreloadExtensions)
	for _, ext := range relevantExtensions {
		normalisedExtensions = append(normalisedExtensions, normaliseIdentifier(ext))
	}

	// Sort extensions alphabetically
	// so that user input doesn't cause
	// more unique parameter group names
	// than necessary
	sort.Strings(normalisedExtensions)

	identifier := fmt.Sprintf(
		"%s-%s-%s",
		config.DBPrefix,
		normalisedFamily,
		config.BrokerName,
	)

	if aws.StringValue(servicePlan.RDSProperties.Engine) == "postgres" && len(normalisedExtensions) > 0 {
		identifier = fmt.Sprintf("%s-%s", identifier, strings.Join(normalisedExtensions, "-"))
	}

	return identifier
}

func filterExtensionsNeedingPreloads(servicePlan ServicePlan, requestedExtensions []string, supportedPreloadExtensions map[string][]DBExtension) []string {
	supportedExtensions := []DBExtension{}
	if exts, ok := supportedPreloadExtensions[aws.StringValue(servicePlan.RDSProperties.EngineFamily)]; ok {
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
