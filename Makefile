# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: list build install image

prefix?= "$(PWD)/projects"
release?= 0
only?= "*"
tag?= "curvebs:unknown"
case?= "*"

list:
	@bash util/build.sh --list

build:
	@bash util/build.sh --only=$(only) --release=$(release)

install:
	@bash util/install.sh --prefix=$(prefix) --only=$(only)

image:
	@bash util/image.sh $(tag)