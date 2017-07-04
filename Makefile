.PHONY: test unit integration

test: unit integration

unit:
	ginkgo -r --skipPackage=ci

integration:
	ginkgo -p --nodes=8 -r ci/blackbox
