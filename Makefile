.PHONY: test unit integration

test: unit integration

POSTGRESQL_PASSWORD=abc123

unit:
	POSTGRESQL_PASSWORD=$(POSTGRESQL_PASSWORD) ginkgo -r --skipPackage=ci

integration:
	#ginkgo -p --nodes=4 -r ci/blackbox
	ginkgo -v -r ci/blackbox

start_docker_dbs:
	docker run -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=$(POSTGRESQL_PASSWORD) -d postgres:9.5
	docker run -p 3306:3306 --name mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -d mysql:5.7
	until docker exec mysql mysqladmin ping --silent; do \
	    printf "."; sleep 1;                             \
	done

stop_docker_dbs:
	docker rm -f postgres
	docker rm -f mysql

