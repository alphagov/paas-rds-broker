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
	"github.com/alphagov/paas-rds-broker/cron"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/sqlengine"
)

func main() {
	var (
		configFilePath = flag.String("config", "", "Location of the config file")
		port           = flag.String("port", "3000", "Listen port")
		cronFlag       = flag.Bool("cron", false, "Start the cron process")
	)
	flag.Parse()

	cfg, err := config.LoadConfig(*configFilePath)
	if err != nil {
		log.Fatalf("Error loading config file: %s", err)
	}
	logger := buildLogger(cfg.LogLevel)
	dbInstance := buildDBInstance(cfg.RDSConfig.Region, logger)

	if *cronFlag {
		err := startCronProcess(cfg, dbInstance, logger)
		if err != nil {
			log.Fatalf("Failed to start cron process: %s", err)
		}
	} else {
		sqlProvider := sqlengine.NewProviderService(logger)
		broker := rdsbroker.New(*cfg.RDSConfig, dbInstance, sqlProvider, logger)
		go broker.CheckAndRotateCredentials()

		err := startHTTPServer(cfg, *port, broker, logger)
		if err != nil {
			log.Fatalf("Failed to start broker process: %s", err)
		}
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

func buildDBInstance(region string, logger lager.Logger) awsrds.DBInstance {
	awsConfig := aws.NewConfig().WithRegion(region).WithMaxRetries(3)
	awsSession := session.New(awsConfig)
	rdssvc := rds.New(awsSession)
	return awsrds.NewRDSDBInstance(region, "aws", rdssvc, logger)
}

func startHTTPServer(
	cfg *config.Config,
	port string,
	serviceBroker *rdsbroker.RDSBroker,
	logger lager.Logger,
) error {
	server := buildHTTPHandler(serviceBroker, logger, cfg)

	// We don't use http.ListenAndServe here so that the "start" log message is
	// logged after the socket is listening. This log message is used by the
	// tests to wait until the broker is ready.
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %s", port, err)
	}

	logger.Info("start", lager.Data{"port": port})
	return http.Serve(listener, server)
}

func startCronProcess(
	cfg *config.Config,
	dbInstance awsrds.DBInstance,
	logger lager.Logger,
) error {
	cronProcess := cron.NewProcess(cfg, dbInstance, logger)
	go stopOnSignal(cronProcess)

	logger.Info("cron.starting")
	return cronProcess.Start()
}

func stopOnSignal(cronProcess *cron.Process) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	if cronProcess != nil {
		cronProcess.Stop()
	}
}
