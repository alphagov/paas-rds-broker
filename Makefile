.PHONY: test unit integration

test: unit integration

unit:
	ginkgo -r --skipPackage=ci

integration:
	ginkgo -r ci/blackbox
