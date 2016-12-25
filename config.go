package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

type Config struct {
	LogLevel  string           `json:"log_level"`
	Username  string           `json:"username"`
	Password  string           `json:"password"`
	GroupName string           `json:"group_name"`
	RDSConfig rdsbroker.Config `json:"rds_config"`
}

func LoadConfig(configFile string) (config *Config, err error) {
	if configFile == "" {
		return config, errors.New("Must provide a config file")
	}

	file, err := os.Open(configFile)
	if err != nil {
		return config, err
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return config, err
	}

	if err = json.Unmarshal(bytes, &config); err != nil {
		return config, err
	}

	config.FillDefaults()
	if err = config.Validate(); err != nil {
		return config, fmt.Errorf("Validating config contents: %s", err)
	}

	return config, nil
}

func (c *Config) Validate() error {
	if c.LogLevel == "" {
		return errors.New("Must provide a non-empty LogLevel")
	}

	if c.Username == "" {
		return errors.New("Must provide a non-empty Username")
	}

	if c.Password == "" {
		return errors.New("Must provide a non-empty Password")
	}

	if err := c.RDSConfig.Validate(); err != nil {
		return fmt.Errorf("Validating RDS configuration: %s", err)
	}

	return nil
}

func (c *Config) FillDefaults() {
	if c.GroupName == "" {
		c.GroupName = "rds_broker"
	}
}
