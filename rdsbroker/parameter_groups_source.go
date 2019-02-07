package rdsbroker

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"regexp"
	"strings"
)

type ParameterGroupSource struct {
	config Config
}

type ParameterGroup struct {
	Name          string   `json:"name"`
	Engine        string   `json:"engine"`
	EngineVersion string   `json:"json_version"`
	Extensions    []string `json:"extensions"`
}

// Represents the criteria to used in
// finding a matching parameter group
type searchCriteria struct {
	Engine                string
	EngineVersion         string
	RequireExtensions     bool
	Extensions            []string
	NumExtensionsRequired int
}

// Holds the state of a search for a
// parameter group
type searchState struct {
	BestCandidate          ParameterGroup
	NumExtensionsSatisfied int
}

func NewParameterGroupSource(config Config) ParameterGroupSource {
	return ParameterGroupSource{config}
}

func (groups *ParameterGroupSource) SelectParameterGroup(servicePlan ServicePlan, parameters ProvisionParameters) (ParameterGroup, error) {
	paramGroups, err := buildParameterGroupsFromNames(groups.config.ParameterGroups, servicePlan)
	if err != nil {
		return ParameterGroup{}, err
	}

	// The version numbers used in service plans contains dots
	// whilst those used in the parameter group names do not
	planEngineVersion := normaliseEngineVersion(aws.StringValue(servicePlan.RDSProperties.EngineVersion))
	planEngine := normaliseEngineName(aws.StringValue(servicePlan.RDSProperties.Engine))
	planEngineString := fmt.Sprintf("%s%s", planEngine, planEngineVersion)

	supportedExtensions := SupportedPreloadExtensions[planEngineString]

	relevantExtensions := filterExtensionsNeedingPreloads(supportedExtensions, parameters.Extensions)

	criteria := searchCriteria{
		Engine:                planEngine,
		EngineVersion:         planEngineVersion,
		RequireExtensions:     len(relevantExtensions) > 0,
		Extensions:            relevantExtensions,
		NumExtensionsRequired: len(relevantExtensions),
	}

	state := searchState{
		BestCandidate:          ParameterGroup{},
		NumExtensionsSatisfied: 0,
	}

	for _, pg := range paramGroups {
		// Only parameter groups with the right engine and version are relevant
		if criteria.Engine == pg.Engine && criteria.EngineVersion == pg.EngineVersion {
			// Some extensions require pre-load libraries,
			// which are set in the parameter group.
			// If the request specified extensions, we
			// must attempt to find a parameter group
			// which satisfies those requirements (at least)
			if criteria.RequireExtensions {
				// Proxy for the best candidate having not been set
				if state.BestCandidate.Name == "" {
					state.BestCandidate = pg
					state.NumExtensionsSatisfied = countSatisfiedExtensions(criteria, pg)
				}

				numSatisfiedExtensions := countSatisfiedExtensions(criteria, pg)
				if numSatisfiedExtensions > state.NumExtensionsSatisfied {
					state.BestCandidate = pg
					state.NumExtensionsSatisfied = numSatisfiedExtensions
				}

				if state.NumExtensionsSatisfied == criteria.NumExtensionsRequired {
					return state.BestCandidate, nil
				}
			} else {
				// Otherwise, the parameter group must have explicitly zero
				// enabled extensions
				if len(pg.Extensions) == 0 {
					state.BestCandidate = pg
				}
			}
		}
	}

	if criteria.RequireExtensions && state.NumExtensionsSatisfied < criteria.NumExtensionsRequired {
		return ParameterGroup{}, fmt.Errorf("cannot find a parameter group with the right extensions enabled. Service plan: %s, extensions: %q", servicePlan.Name, parameters.Extensions)
	}

	if state.BestCandidate.Name == "" {
		return ParameterGroup{}, fmt.Errorf("unable to find a parameter group with the right engine and no other extensions enabled. Service plan: %s", servicePlan.Name)
	}

	return state.BestCandidate, nil
}

func normaliseEngineVersion(value string) string {
	return strings.Replace(value, ".", "", -1)
}

func normaliseEngineName(value string) string {
	return strings.Replace(value, "-", "", -1)
}

func filterExtensionsNeedingPreloads(supportedExtensions []DBExtension, requestedExtensions []string) []string {
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

func buildParameterGroupsFromNames(groupNames []string, servicePlan ServicePlan) ([]ParameterGroup, error) {
	var paramGroups []ParameterGroup
	for _, g := range groupNames {
		pg, err := decodeName(g, servicePlan)

		if err != nil {
			return []ParameterGroup{}, err
		}

		paramGroups = append(paramGroups, pg)
	}

	return paramGroups, nil
}

func countSatisfiedExtensions(criteria searchCriteria, group ParameterGroup) int {
	i := 0
	for _, required := range criteria.Extensions {
		for _, ext := range group.Extensions {
			if required == ext {
				i++
				break
			}
		}
	}

	return i
}

// decodes names in the format:
// rdsbroker-engine_version-envname-ext-ensions-list
func decodeName(name string, servicePlan ServicePlan) (ParameterGroup, error) {
	expr := regexp.MustCompile("(?P<app>rdsbroker)-(?P<engine_name>[A-Za-z]+)(?P<engine_version>[0-9]+)-(?P<env>[A-Za-z0-9]{1,8})(-(?P<extensions>[A-Za-z0-9-]*))?")
	success, matches := tryMatchExpressionWithNamedGroups(expr, name)

	if !success {
		return ParameterGroup{}, fmt.Errorf("parameter group name %s doesn't contain the relevant fields", name)
	}

	normalisedEngineName := normaliseEngineName(aws.StringValue(servicePlan.RDSProperties.Engine))
	normalisedEngineVersion := normaliseEngineVersion(aws.StringValue(servicePlan.RDSProperties.EngineVersion))
	engineExtensionVersion := fmt.Sprintf("%s%s", normalisedEngineName, normalisedEngineVersion)
	supportedExtensions := SupportedPreloadExtensions[engineExtensionVersion]

	engine := matches["engine_name"]
	engineVersion := matches["engine_version"]
	extensions := findExtensions(matches["extensions"], supportedExtensions)

	return ParameterGroup{
		Name:          name,
		Engine:        engine,
		EngineVersion: engineVersion,
		Extensions:    extensions,
	}, nil
}

func tryMatchExpressionWithNamedGroups(expr *regexp.Regexp, str string) (bool, map[string]string) {
	// https://stackoverflow.com/a/20751656
	result := make(map[string]string)
	matches := expr.FindStringSubmatch(str)
	if matches == nil {
		return false, result
	}

	for i, name := range expr.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = matches[i]
		}
	}

	return true, result
}

func findExtensions(paramGroupExtensions string, supportedExtensions []DBExtension) []string {
	var extensions = []string{}

	// Extension names cannot contain underscores in property group names,
	// but postgres extension names use underscores by convention.
	// To work around this, underscores and substituted for hyphens in
	// those names.
	// Normalisation undoes that
	var normalisedParamGroupExtensions = strings.Replace(paramGroupExtensions, "-", "_", -1)

	for _, ext := range supportedExtensions {
		if strings.Contains(normalisedParamGroupExtensions, ext.Name) {
			extensions = append(extensions, ext.Name)
		}
	}

	return extensions
}
