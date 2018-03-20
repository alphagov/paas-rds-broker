package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

type Config struct {
	LogLevel             string            `json:"log_level"`
	Username             string            `json:"username"`
	Password             string            `json:"password"`
	RDSConfig            *rdsbroker.Config `json:"rds_config"`
	KeepSnapshotsForDays int               `json:"keep_snapshots_for_days"`
	CronSchedule         string            `json:"cron_schedule"`
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

func (c Config) FillDefaults() {
	c.RDSConfig.FillDefaults()
}

func (c Config) Validate() error {
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

	if c.KeepSnapshotsForDays <= 0 {
		return errors.New("must provide a valid number for keep_snapshots_for_days")
	}

	if c.CronSchedule == "" {
		return errors.New("must provide a non-empty cron_schedule")
	}

	return nil
}
