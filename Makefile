# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: list build dep install image

stor?=""
prefix?= "$(PWD)/projects"
release?= 0
dep?= 0
only?= "*"
tag?= "curvebs:unknown"
case?= "*"
os?= "debian9"

define help_msg
## list
Usage:
    make list stor=bs/fs
Examples:
    make list stor=bs

## build
Usage:
    make build stor=bs/fs only=TARGET dep=0/1 release=0/1 os=OS
Examples:
    make build stor=bs only=//src/chunkserver:chunkserver
    make build stor=bs only=src/* dep=0
    make build stor=fs only=test/* os=debian9
    make build stor=fs release=1


## dep
Usage:
    make dep stor=bs/fs
Examples:
    make dep stor=bs


## install
Usage:
    make install stor=bs/fs prefix=PREFIX only=TARGET
Examples:
    make install stor=bs prefix=/usr/local/curvebs only=*
    make install stor=bs prefix=/usr/local/curvebs only=chunkserver
    make install stor=fs prefix=/usr/local/curvefs only=etcd


## image
Usage:
    make image stor=bs/fs tag=TAG os=OS
Examples:
    make image stor=bs tag=opencurvedocker/curvebs:v1.2 os=debian9
endef
export help_msg

help:
	@echo "$$help_msg"

list:
	@bash util/build.sh --stor=$(stor) --list

build:
	@bash util/build.sh --stor=$(stor) --only=$(only) --dep=$(dep) --release=$(release) --os=$(os)

dep:
	@bash util/build.sh --stor=$(stor) --only="" --dep=1

install:
	@bash util/install.sh --stor=$(stor) --prefix=$(prefix) --only=$(only)

image:
	@bash util/image.sh $(stor) $(tag) $(os)
