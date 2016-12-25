package sqlengine

import (
	"fmt"
	"strings"

	"github.com/pivotal-golang/lager"
)

type ProviderService struct {
	logger    lager.Logger
	groupName string
}

func NewProviderService(logger lager.Logger, groupName string) *ProviderService {
	return &ProviderService{
		logger:    logger,
		groupName: groupName,
	}
}

func (p *ProviderService) GetSQLEngine(engine string) (SQLEngine, error) {
	switch strings.ToLower(engine) {
	case "mariadb", "mysql":
		return NewMySQLEngine(p.logger), nil
	case "postgres", "postgresql":
		return NewPostgresEngine(p.logger, p.groupName), nil
	}

	return nil, fmt.Errorf("SQL Engine '%s' not supported", engine)
}
