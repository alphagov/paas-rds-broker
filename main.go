package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/pivotal-cf/brokerapi"

	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/config"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/sqlengine"
	"github.com/robfig/cron"
)

var (
	configFilePath string
	port           string
	cronFlag       bool
	cronProcess    *cron.Cron

	logLevels = map[string]lager.LogLevel{
		"DEBUG": lager.DEBUG,
		"INFO":  lager.INFO,
		"ERROR": lager.ERROR,
		"FATAL": lager.FATAL,
	}
)

func init() {
	flag.StringVar(&configFilePath, "config", "", "Location of the config file")
	flag.StringVar(&port, "port", "3000", "Listen port")
	flag.BoolVar(&cronFlag, "cron", false, "Start the cron process")
}

func buildLogger(logLevel string) lager.Logger {
	laggerLogLevel, ok := logLevels[strings.ToUpper(logLevel)]
	if !ok {
		log.Fatal("Invalid log level: ", logLevel)
	}

	logger := lager.NewLogger("rds-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, laggerLogLevel))

	return logger
}

func buildHTTPHandler(serviceBroker *rdsbroker.RDSBroker, logger lager.Logger, config *config.Config) http.Handler {
	credentials := brokerapi.BrokerCredentials{
		Username: config.Username,
		Password: config.Password,
	}

	brokerAPI := brokerapi.New(serviceBroker, logger, credentials)
	mux := http.NewServeMux()
	mux.Handle("/", brokerAPI)
	mux.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func main() {
	flag.Parse()

	config, err := config.LoadConfig(configFilePath)
	if err != nil {
		log.Fatalf("Error loading config file: %s", err)
	}

	logger := buildLogger(config.LogLevel)
	awsConfig := aws.NewConfig().WithRegion(config.RDSConfig.Region)
	awsSession := session.New(awsConfig)
	rdssvc := rds.New(awsSession)
	dbInstance := awsrds.NewRDSDBInstance(config.RDSConfig.Region, config.RDSConfig.AWSPartition, rdssvc, logger)
	sqlProvider := sqlengine.NewProviderService(logger)

	go stopOnSignal()

	if cronFlag {
		err := startCron(config, dbInstance, logger)
		if err != nil {
			log.Fatalf("Failed to start cron process: %s", err)
		}
	} else {
		err := startServiceBroker(config, dbInstance, sqlProvider, logger)
		if err != nil {
			log.Fatalf("Failed to start broker process: %s", err)
		}
	}
}

func startServiceBroker(
	config *config.Config,
	dbInstance awsrds.DBInstance,
	sqlProvider sqlengine.Provider,
	logger lager.Logger,
) error {
	serviceBroker := rdsbroker.New(*config.RDSConfig, dbInstance, sqlProvider, logger)

	go serviceBroker.CheckAndRotateCredentials()

	server := buildHTTPHandler(serviceBroker, logger, config)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %s", port, err)
	}

	logger.Info("start", lager.Data{"port": port})
	return http.Serve(listener, server)
}

func startCron(
	config *config.Config,
	dbInstance awsrds.DBInstance,
	logger lager.Logger,
) error {
	cronProcess = cron.New()
	err := cronProcess.AddFunc(config.CronSchedule, func() {
		err := dbInstance.DeleteSnapshots(config.RDSConfig.BrokerName, config.KeepSnapshotsForDays)
		if err != nil {
			logger.Error("delete-snapshots", err)
		}
	})
	if err != nil {
		return fmt.Errorf("cron_schedule is invalid: %s", err)
	}

	logger.Info("cron-start")
	cronProcess.Run()
	logger.Info("cron-stop")

	return nil
}

func stopCron() {
	if cronProcess != nil {
		cronProcess.Stop()
	}
}

func stopOnSignal() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	stopCron()
}
