package cron

import (
	"fmt"

	"code.cloudfoundry.org/lager"
	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/config"
	robfig_cron "github.com/robfig/cron"
)

type Process struct {
	cron       *robfig_cron.Cron
	config     *config.Config
	dbInstance awsrds.RDSInstance
	logger     lager.Logger
}

func NewProcess(config *config.Config, dbInstance awsrds.RDSInstance, logger lager.Logger) *Process {
	return &Process{
		config:     config,
		dbInstance: dbInstance,
		logger:     logger,
	}
}

func (p *Process) Start() error {
	p.cron = robfig_cron.New()
	err := p.cron.AddFunc(p.config.CronSchedule, func() {
		err := p.dbInstance.DeleteSnapshots(p.config.RDSConfig.BrokerName, p.config.KeepSnapshotsForDays)
		if err != nil {
			p.logger.Error("delete-snapshots", err)
		}
	})
	if err != nil {
		return fmt.Errorf("cron_schedule is invalid: %s", err)
	}

	p.logger.Info("cron-start")
	p.cron.Run()
	p.logger.Info("cron-stop")

	return nil
}

func (p *Process) Stop() {
	if p.cron != nil {
		p.cron.Stop()
	}
}
