package config

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
)

type SSLConfig struct {
	CertificateContent string `json:"certificate"`
	KeyContent         string `json:"key"`
	Certificate        *tls.Certificate
}
type Config struct {
	Port                 int               `json:"port"`
	LogLevel             string            `json:"log_level"`
	Username             string            `json:"username"`
	Password             string            `json:"password"`
	RunHousekeeping      bool              `json:"run_housekeeping"`
	KeepSnapshotsForDays int               `json:"keep_snapshots_for_days"`
	CronSchedule         string            `json:"cron_schedule"`
	RDSConfig            *rdsbroker.Config `json:"rds_config"`
	SSLConfig            *SSLConfig        `json:"ssl_config"`
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
		c.Port = 3000
	}
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

	if c.KeepSnapshotsForDays <= 0 {
		return errors.New("must provide a valid number for keep_snapshots_for_days")
	}

	if c.CronSchedule == "" {
		return errors.New("must provide a non-empty cron_schedule")
	}

	if err := c.RDSConfig.Validate(); err != nil {
		return fmt.Errorf("Validating RDS configuration: %s", err)
	}

	if c.SSLConfig != nil {
		if c.SSLConfig.CertificateContent == "" {
			return errors.New("must provide a non-empty ssl_config.certificate")
		}

		if c.SSLConfig.KeyContent == "" {
			return errors.New("must provide a non-empty ssl_config.key")
		}

		cert, err := tls.X509KeyPair([]byte(c.SSLConfig.CertificateContent), []byte(c.SSLConfig.KeyContent))
		if err != nil {
			return fmt.Errorf("parsing ssl_config contents: %w", err)
		}
		c.SSLConfig.Certificate = &cert
	}

	return nil
}
