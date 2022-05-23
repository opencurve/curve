# Copyright (C) 2022 Jingli Chen (Wine93), NetEase Inc.

.PHONY: build init proto clean

release?= 0
version?= "unknown"

build:
	@if [ ${release} = 1 ];then\
		go build -ldflags "-w -s \
		-X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=${version}" \
		-o sbin/curve $(PWD)/cmd/curvecli/main.go;\
	else\
		go build -ldflags \
		"-X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=${version}" \
		-o sbin/curve $(PWD)/cmd/curvecli/main.go;\
	fi

init: proto
	go mod init github.com/opencurve/curve/tools-v2
	go mod tidy

proto:
	@bash mk-proto.sh

clean:
	rm -rf sbin
	rm -rf proto/*
	rm -rf go.mod
	rm -rf go.sum
