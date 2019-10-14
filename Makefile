POSTGRESQL_PASSWORD=abc123
MYSQL_PASSWORD=toor

.PHONY: integration
integration:
	ginkgo -p --nodes=9 -r ci/blackbox --slowSpecThreshold=900 -stream

.PHONY: unit
unit: test_unit test_all_sql

.PHONY: test_unit
test_unit:
	ginkgo -r --skipPackage=ci,sqlengine
.PHONY: test_all_sql
test_all_sql: test_postgres test_mysql
.PHONY: test_postgres
test_postgres: start_postgres_9 run_postgres_sql_tests stop_postgres_9 start_postgres_10 run_postgres_sql_tests stop_postgres_10 start_postgres_11 run_postgres_sql_tests stop_postgres_11
.PHONY: test_mysql
test_mysql: start_mysql_80 run_mysql_sql_tests stop_mysql_80 start_mysql_57 run_mysql_sql_tests stop_mysql_57

.PHONY: run_mysql_sql_tests
run_mysql_sql_tests:
	MYSQL_PASSWORD=$(MYSQL_PASSWORD) ginkgo -focus=MySQLEngine.* sqlengine/

.PHONY: run_postgres_sql_tests
run_postgres_sql_tests:
	POSTGRESQL_PASSWORD=$(POSTGRESQL_PASSWORD) ginkgo -focus=PostgresEngine.* sqlengine/

.PHONY: start_postgres_9
start_postgres_9:
	docker run -p 5432:5432 --name postgres-9 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:9.5; \
	sleep 5

.PHONY: start_postgres_10
start_postgres_10:
	docker run -p 5432:5432 --name postgres-10 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:10.5; \
	sleep 5

.PHONY: start_postgres_11
start_postgres_11:
	docker run -p 5432:5432 --name postgres-11 -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:11.5; \
	sleep 5

.PHONY: stop_postgres_9
stop_postgres_9:
	docker rm -f postgres-9

.PHONY: stop_postgres_10
stop_postgres_10:
	docker rm -f postgres-10

.PHONY: stop_postgres_11
stop_postgres_11:
	docker rm -f postgres-11

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
	docker rm -f postgres-9 || true
	docker rm -f postgres-10 || true
	docker rm -f postgres-11 || true
