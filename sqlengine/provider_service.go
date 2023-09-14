package sqlengine

import (
	"fmt"
	"strings"

	"code.cloudfoundry.org/lager/v3"
)

type ProviderService struct {
	logger lager.Logger
}

func NewProviderService(logger lager.Logger) *ProviderService {
	return &ProviderService{
		logger: logger,
	}
}

func (p *ProviderService) GetSQLEngine(engine string) (SQLEngine, error) {
	switch strings.ToLower(engine) {
	case "mariadb", "mysql":
		return NewMySQLEngine(p.logger), nil
	case "postgres", "postgresql":
		return NewPostgresEngine(p.logger), nil
	}

	return nil, fmt.Errorf("SQL Engine '%s' not supported", engine)
}
