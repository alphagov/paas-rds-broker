package rdsbroker

import (
	"errors"
	"fmt"
)

type Config struct {
	Region                       string  `json:"region"`
	DBPrefix                     string  `json:"db_prefix"`
	BrokerName                   string  `json:"broker_name"`
	MasterPasswordSeed           string  `json:"master_password_seed"`
	AllowUserProvisionParameters bool    `json:"allow_user_provision_parameters"`
	AllowUserUpdateParameters    bool    `json:"allow_user_update_parameters"`
	AllowUserBindParameters      bool    `json:"allow_user_bind_parameters"`
	Catalog                      Catalog `json:"catalog"`
}

func (c Config) Validate() error {
	if c.Region == "" {
		return errors.New("Must provide a non-empty Region")
	}

	if c.DBPrefix == "" {
		return errors.New("Must provide a non-empty DBPrefix")
	}

	if c.BrokerName == "" {
		return errors.New("Must provide a non-empty BrokerName")
	}

	if c.MasterPasswordSeed == "" {
		return errors.New("Must provide a non-empty MasterPasswordSeed")
	}

	if err := c.Catalog.Validate(); err != nil {
		return fmt.Errorf("Validating Catalog configuration: %s", err)
	}

	return nil
}
