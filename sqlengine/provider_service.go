package sqlengine

import (
	"fmt"
	"strings"

	"code.cloudfoundry.org/lager"
)

type ProviderService struct {
	logger             lager.Logger
	stateEncryptionKey string
}

func NewProviderService(logger lager.Logger, stateEncryptionKey string) *ProviderService {
	return &ProviderService{
		logger:             logger,
		stateEncryptionKey: stateEncryptionKey,
	}
}

func (p *ProviderService) GetSQLEngine(engine string) (SQLEngine, error) {
	switch strings.ToLower(engine) {
	case "mariadb", "mysql":
		return NewMySQLEngine(p.logger), nil
	case "postgres", "postgresql":
		return NewPostgresEngine(p.logger, p.stateEncryptionKey), nil
	}

	return nil, fmt.Errorf("SQL Engine '%s' not supported", engine)
}
