package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/pivotal-cf/brokerapi/v8"

	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/config"
	"github.com/alphagov/paas-rds-broker/cron"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/sqlengine"
)

func main() {
	configFilePath := flag.String("config", "", "Location of the config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configFilePath)
	if err != nil {
		log.Fatalf("Error loading config file: %s", err)
	}
	logger := buildLogger(cfg.LogLevel)
	dbInstance := buildDBInstance(*cfg.RDSConfig, logger)
	sqlProvider := sqlengine.NewProviderService(logger)
	parameterGroupSource := rdsbroker.NewParameterGroupSource(*cfg.RDSConfig, dbInstance, rdsbroker.SupportedPreloadExtensions, logger.Session("parameter_group_source"))
	broker := rdsbroker.New(*cfg.RDSConfig, dbInstance, sqlProvider, parameterGroupSource, logger)

	if cfg.RunHousekeeping {
		go broker.CheckAndRotateCredentials()
		go startCronProcess(cfg, dbInstance, logger)
	}

	err = startHTTPServer(cfg, broker, logger)
	if err != nil {
		log.Fatalf("Failed to start broker process: %s", err)
	}
}

func buildLogger(logLevel string) lager.Logger {
	lagerLogLevel, err := lager.LogLevelFromString(strings.ToLower(logLevel))
	if err != nil {
		log.Fatal(err)
	}

	logger := lager.NewLogger("rds-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lagerLogLevel))

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

func buildDBInstance(rdsCfg rdsbroker.Config, logger lager.Logger) awsrds.RDSInstance {
	awsConfig := aws.NewConfig().WithRegion(rdsCfg.Region).WithMaxRetries(3)
	awsSession, _ := session.NewSession(awsConfig)
	rdssvc := rds.New(awsSession)
	return awsrds.NewRDSDBInstance(
		rdsCfg.Region,
		"aws",
		rdssvc,
		logger,
		time.Second*time.Duration(rdsCfg.AWSTagCacheSeconds),
		nil,
	)
}

func startHTTPServer(
	cfg *config.Config,
	serviceBroker *rdsbroker.RDSBroker,
	logger lager.Logger,
) error {
	server := buildHTTPHandler(serviceBroker, logger, cfg)

	// We don't use http.ListenAndServe here so that the "start" log message is
	// logged after the socket is listening. This log message is used by the
	// tests to wait until the broker is ready.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %s", cfg.Port, err)
	}
	if cfg.SSLConfig != nil {
		listener = tls.NewListener(listener, &tls.Config{
			Certificates: []tls.Certificate{*cfg.SSLConfig.Certificate},
		})
	}
	logger.Info("start", lager.Data{"port": cfg.Port})
	return http.Serve(listener, server)
}

func startCronProcess(
	cfg *config.Config,
	dbInstance awsrds.RDSInstance,
	logger lager.Logger,
) {
	cronProcess := cron.NewProcess(cfg, dbInstance, logger)
	go stopOnSignal(cronProcess)

	logger.Info("cron.starting")
	err := cronProcess.Start()
	if err != nil {
		log.Fatalf("Failed to start cron process: %s", err)
	}
}

func stopOnSignal(cronProcess *cron.Process) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	if cronProcess != nil {
		cronProcess.Stop()
	}
}
