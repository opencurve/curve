# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: build dep install image

prefix?= "$(PWD)/devops/projects"
only?= "*"
dep?= 0
release?= 0
os?= "debian9"
build_rocksdb?= 0
tag?= "curvefs:unknown"

build:
	@bash util/build.sh --only=$(only) --dep=$(dep) --release=$(release) --os=$(os) --build_rocksdb=$(build_rocksdb)

dep:
	@bash util/build.sh --only="" --dep=1 --build_rocksdb=$(build_rocksdb)

install:
	@bash util/install.sh --prefix=$(prefix) --only=$(only)

image:
	@bash util/image.sh $(tag) $(os)
