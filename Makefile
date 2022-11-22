git # Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: list build dep install image

stor?=""
prefix?= "$(PWD)/projects"
release?= 0
only?= "*"
tag?= "curvebs:unknown"
case?= "*"
os?= "debian9"

list:
	@bash util/build.sh --stor=${stor} --list

build:
	@bash util/build.sh --stor=${stor} --only=$(only) --dep=$(dep) --release=$(release) --os=$(os)

dep:
	@bash util/build.sh --stor=${stor} --only="" --dep=1

install:
	@bash util/install.sh --stor=${stor} --prefix=$(prefix) --only=$(only)

image:
	@bash util/image.sh ${stor} $(tag) $(os)
