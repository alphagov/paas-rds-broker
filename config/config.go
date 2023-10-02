package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

const (
	DefaultPort = 3000
	DefaultHost = "0.0.0.0"
)

type Config struct {
	Port                 int               `json:"port"`
	LogLevel             string            `json:"log_level"`
	Username             string            `json:"username"`
	Password             string            `json:"password"`
	Host                 string            `json:"host"`
	RunHousekeeping      bool              `json:"run_housekeeping"`
	KeepSnapshotsForDays int               `json:"keep_snapshots_for_days"`
	CronSchedule         string            `json:"cron_schedule"`
	RDSConfig            *rdsbroker.Config `json:"rds_config"`
	TLS                  *TLSConfig        `json:"tls"`
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

	if err = json.NewDecoder(file).Decode(&config); err != nil {
		return config, err
	}

	config.FillDefaults()

	if err = config.Validate(); err != nil {
		return config, fmt.Errorf("Validating config contents: %s", err)
	}

	return config, nil
}

func (c *Config) FillDefaults() {
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.Host == "" {
		c.Host = DefaultHost
	}
	c.RDSConfig.FillDefaults()
}

func (c Config) TLSEnabled() bool {
	return c.TLS != nil
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

	if c.KeepSnapshotsForDays <= 0 {
		return errors.New("must provide a valid number for keep_snapshots_for_days")
	}

	if c.CronSchedule == "" {
		return errors.New("must provide a non-empty cron_schedule")
	}

	if err := c.RDSConfig.Validate(); err != nil {
		return fmt.Errorf("Validating RDS configuration: %s", err)
	}

	if c.TLS != nil {
		tlsValidation := c.TLS.validate()
		if tlsValidation != nil {
			return tlsValidation
		}
	}

	return nil
}
