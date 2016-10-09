package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pivotal-cf/brokerapi"
	"code.cloudfoundry.org/lager"

	"github.com/alphagov/paas-rds-broker/awsrds"
	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/sqlengine"
)

var (
	configFilePath string
	port           string

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

func buildHTTPHandler(serviceBroker *rdsbroker.RDSBroker, logger lager.Logger, config *Config) http.Handler {
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

	config, err := LoadConfig(configFilePath)
	if err != nil {
		log.Fatalf("Error loading config file: %s", err)
	}

	logger := buildLogger(config.LogLevel)

	awsConfig := aws.NewConfig().WithRegion(config.RDSConfig.Region)
	awsSession := session.New(awsConfig)

	rdssvc := rds.New(awsSession)
	stssvc := sts.New(awsSession)

	dbInstance := awsrds.NewRDSDBInstance(config.RDSConfig.Region, rdssvc, stssvc, logger)

	sqlProvider := sqlengine.NewProviderService(logger, config.StateEncryptionKey)

	serviceBroker := rdsbroker.New(config.RDSConfig, dbInstance, sqlProvider, logger)

	go serviceBroker.CheckAndRotateCredentials()

	server := buildHTTPHandler(serviceBroker, logger, config)

	fmt.Println("RDS Service Broker started on port " + port + "...")
	http.ListenAndServe(":"+port, server)
}
