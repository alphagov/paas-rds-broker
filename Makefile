.PHONY: test unit integration

test: unit integration

unit:
	ginkgo -r --skipPackage=ci

integration:
	$(if ${DB_SUBNET_GROUP_NAME},,$(error Must pass DB_SUBNET_GROUP_NAME=<name>))
	$(if ${VPC_SECURITY_GROUP_ID},,$(error Must pass VPC_SECURITY_GROUP_ID=<id>))
	cp ci/blackbox/test_config.json ci/blackbox/config.json
	sed -i -e "s/DB_SUBNET_GROUP_NAME/${DB_SUBNET_GROUP_NAME}/g" ci/blackbox/config.json
	sed -i -e "s/VPC_SECURITY_GROUP_ID/${VPC_SECURITY_GROUP_ID}/g" ci/blackbox/config.json
	ginkgo -r ci/blackbox
	rm ci/blackbox/config.json
