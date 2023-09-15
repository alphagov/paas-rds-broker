POSTGRESQL_PASSWORD=abc123
MYSQL_PASSWORD=toor

.PHONY: integration
integration:
	go run github.com/onsi/ginkgo/v2/ginkgo --nodes=4 -r ci/blackbox --slowSpecThreshold=1800 -stream -failFast -mod=readonly

.PHONY: unit
unit: test_unit test_all_sql

.PHONY: test_unit
test_unit:
	go run github.com/onsi/ginkgo/v2/ginkgo -r --skip-package=ci,sqlengine,release -mod=readonly
.PHONY: test_all_sql
test_all_sql: test_postgres test_mysql
.PHONY: test_postgres
test_postgres: run_postgres_sql_tests start_postgres_10 run_postgres_sql_tests stop_postgres_10 start_postgres_11 run_postgres_sql_tests stop_postgres_11 start_postgres_12 run_postgres_sql_tests stop_postgres_12 start_postgres_13 run_postgres_sql_tests stop_postgres_13
.PHONY: test_mysql
test_mysql: start_mysql_80 run_mysql_sql_tests stop_mysql_80 start_mysql_57 run_mysql_sql_tests stop_mysql_57

.PHONY: run_mysql_sql_tests
run_mysql_sql_tests:
	MYSQL_PASSWORD=$(MYSQL_PASSWORD) go run github.com/onsi/ginkgo/v2/ginkgo -focus=MySQLEngine.* sqlengine/

.PHONY: run_postgres_sql_tests
run_postgres_sql_tests:
	POSTGRESQL_PASSWORD=$(POSTGRESQL_PASSWORD) go run github.com/onsi/ginkgo/v2/ginkgo -focus=PostgresEngine.* sqlengine/

.PHONY: start_postgres_10
start_postgres_10:
	docker run -p 5432:5432 --name postgres-10 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:10.5; \
	sleep 5

.PHONY: start_postgres_11
start_postgres_11:
	docker run -p 5432:5432 --name postgres-11 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:11.5; \
	sleep 5

.PHONY: start_postgres_12
start_postgres_12:
	docker run -p 5432:5432 --name postgres-12 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:12.5; \
	sleep 5

.PHONY: start_postgres_13
start_postgres_13:
	docker run -p 5432:5432 --name postgres-13 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:13; \
	sleep 5

.PHONY: stop_postgres_10
stop_postgres_10:
	docker rm -f postgres-10

.PHONY: stop_postgres_11
stop_postgres_11:
	docker rm -f postgres-11

.PHONY: stop_postgres_12
stop_postgres_12:
	docker rm -f postgres-12

.PHONY: stop_postgres_13
stop_postgres_13:
	docker rm -f postgres-13

.PHONY: start_mysql_57
start_mysql_57:
	docker run -p 3307:3306 --name mysql-57 -e MYSQL_ROOT_PASSWORD=$(MYSQL_PASSWORD) -d mysql:5.7; \
	until docker exec mysql-57 mysqladmin ping --silent; do \
	    printf "."; sleep 1;                             \
	done; \
	sleep 5

.PHONY: start_mysql_80
start_mysql_80:
	docker run -p 3307:3306 --name mysql-80 -e MYSQL_ROOT_PASSWORD=$(MYSQL_PASSWORD) -d mysql:8.0 \
	    --default-authentication-plugin=mysql_native_password; \
	until docker exec mysql-80 mysqladmin ping --silent; do \
		printf "."; sleep 1;                             \
	done; \
	sleep 5

.PHONY: stop_mysql_57
stop_mysql_57:
	docker rm -f mysql-57

.PHONY: stop_mysql_80
stop_mysql_80:
	docker rm -f mysql-80

.PHONY: stop_dbs
stop_dbs:
	docker rm -f mysql-57 || true
	docker rm -f mysql-80 || true
	docker rm -f postgres-10 || true
	docker rm -f postgres-11 || true
	docker rm -f postgres-12 || true

.PHONY: bosh_release
bosh_release:
	$(eval export VERSION ?= 0.0.$(shell date +"%s"))
	$(eval export REGION ?= ${AWS_DEFAULT_REGION})
	$(eval export BUCKET ?= gds-paas-build-releases)
	$(eval export TARBALL_DIR ?= bosh-release-tarballs)
	$(eval export TARBALL_NAME = rds-broker-${VERSION}.tgz)
	$(eval export TARBALL_PATH = ${TARBALL_DIR}/${TARBALL_NAME})

	@[ -d "${TARBALL_DIR}" ] || mkdir "${TARBALL_DIR}"
	@[ -d "release/src/github.com/alphagov/paas-rds-broker" ] || mkdir "release/src/github.com/alphagov/paas-rds-broker"

	@rm -rf release/src/github.com/alphagov/paas-rds-broker/*

	# rsync doesn't exist in the container
	# which is used in CI for building the
	# bosh release. Creating and extracting
	# a tar archive is a simple enough replacement.
	git ls-files \
	| grep -v "release/" \
	| tar cf broker.tz -T -

	tar xf broker.tz -C release/src/github.com/alphagov/paas-rds-broker
	rm broker.tz

	bosh create-release \
		--name "rds-broker" \
		--version "${VERSION}" \
		--tarball "${TARBALL_PATH}" \
		--dir release \
		--force

	ls -al ${TARBALL_DIR}

	@# Can't use heredoc in Make target
	@echo "releases:"
	@echo "  - name: rds-broker"
	@echo "    version: ${VERSION}"
	@echo "    url: https://s3-${REGION}.amazonaws.com/$${BUCKET}/rds-broker-${VERSION}.tgz"
	@echo "    sha1: $$(openssl sha1 "${TARBALL_PATH}" | cut -d' ' -f 2)"

.PHONY: build_amd64
build_amd64:
	mkdir -p amd64
	GOOS=linux GOARCH=amd64 go build -o amd64/paas-rds-broker

.PHONY: bosh_scp
bosh_scp: build_amd64
	./scripts/bosh-scp.sh
