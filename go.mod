module github.com/alphagov/paas-rds-broker

go 1.13

require (
	code.cloudfoundry.org/lager v1.1.0
	github.com/aws/aws-sdk-go v1.16.34
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-sql-driver/mysql v1.2.1-0.20150908124601-527bcd55aab2
	github.com/gorilla/context v0.0.0-20141217160251-215affda49ad
	github.com/gorilla/mux v0.0.0-20150213192255-8a875a034c69
	github.com/hpcloud/tail v1.0.0
	github.com/jmespath/go-jmespath v0.0.0-20180206201540-c2b33e8439af
	github.com/lib/pq v0.0.0-20151007185736-ffe986aba3e6
	github.com/onsi/ginkgo v1.7.0
	github.com/onsi/gomega v1.4.3
	github.com/phayes/freeport v0.0.0-20141201041908-e7681b376149
	github.com/pivotal-cf/brokerapi v0.0.0-20170224155331-b90ba3f93800
	github.com/robfig/cron v1.0.1-0.20171101201047-2315d5715e36
	github.com/satori/go.uuid v1.1.1-0.20160927100844-b061729afc07
	golang.org/x/net v0.0.0-20190206173232-65e2d4e15006
	golang.org/x/sys v0.0.0-20161214190518-d75a52659825
	golang.org/x/text v0.3.0
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7
	gopkg.in/yaml.v2 v2.0.0-20160928153709-a5b47d31c556
)

replace gopkg.in/fsnotify.v1 v1.4.7 => github.com/fsnotify/fsnotify v1.4.7
