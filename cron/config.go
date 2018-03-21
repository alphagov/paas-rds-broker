package cron

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

type Config struct {
	LogLevel             string    `json:"log_level"`
	KeepSnapshotsForDays int       `json:"keep_snapshots_for_days"`
	CronSchedule         string    `json:"cron_schedule"`
	RDSConfig            RDSConfig `json:"rds_config"`
}

type RDSConfig struct {
	Region     string `json:"region"`
	BrokerName string `json:"broker_name"`
}

func LoadConfig(configFile string) (*Config, error) {
	config := &Config{}

	if configFile == "" {
		return nil, errors.New("must provide a config file")
	}

	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(bytes, config); err != nil {
		return nil, err
	}

	if err = config.Validate(); err != nil {
		return nil, fmt.Errorf("validating config contents: %s", err)
	}

	return config, nil
}

func (c Config) Validate() error {
	if c.LogLevel == "" {
		return errors.New("must provide a non-empty log_level")
	}

	if c.RDSConfig.Region == "" {
		return errors.New("must provide a non-empty rds_config.region")
	}

	if c.RDSConfig.BrokerName == "" {
		return errors.New("must provide a non-empty rds_config.broker_name")
	}

	if c.KeepSnapshotsForDays <= 0 {
		return errors.New("must provide a valid number for keep_snapshots_for_days")
	}

	if c.CronSchedule == "" {
		return errors.New("must provide a non-empty cron_schedule")
	}

	return nil
}
