.PHONY: build

curve-sdk:
	docker build --target  curve-sdk --no-cache -t opencurvedocker/curve-base:curve-sdk-debian11 .

build:
	docker build --target  curve-spdk-tgt --no-cache -t opencurvedocker/curve-base:curve-spdk-tgt-debian11 .
