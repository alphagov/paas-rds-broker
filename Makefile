.PHONY: unit integration run_unit run_sql_tests start_postgres_9 start_postgres_10 stop_postgres_9 stop_postgres_10 start_mysql stop_mysql stop_dbs

POSTGRESQL_PASSWORD=abc123

integration:
	ginkgo -p --nodes=9 -r ci/blackbox -slowSpecThreshold 900

unit: start_postgres_9 start_mysql run_unit stop_postgres_9 start_postgres_10 run_sql_tests stop_postgres_10 stop_mysql

run_unit:
	POSTGRESQL_PASSWORD=$(POSTGRESQL_PASSWORD) ginkgo -r --skipPackage=ci

run_sql_tests:
	POSTGRESQL_PASSWORD=$(POSTGRESQL_PASSWORD) ginkgo sqlengine/

start_postgres_9:
	docker run -p 5432:5432 --name postgres-9 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:9.5; \
	sleep 5

start_postgres_10:
	docker run -p 5432:5432 --name postgres-10 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:10.5; \
	sleep 5

stop_postgres_9:
	docker rm -f postgres-9

stop_postgres_10:
	docker rm -f postgres-10

start_mysql:
	docker run -p 3306:3306 --name mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -d mysql:5.7; \
	until docker exec mysql mysqladmin ping --silent; do \
	    printf "."; sleep 1;                             \
	done; \
	sleep 5

stop_mysql:
	docker rm -f mysql

stop_dbs:
	docker rm -f mysql || true
	docker rm -f postgres-9 || true
	docker rm -f postgres-10 || true
