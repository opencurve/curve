# Copyright (C) 2022 Jingli Chen (Wine93), NetEase Inc.

.PHONY: build init proto clean

release?= 0
version?= "unknown"

build: proto
	@if [ ${release} = 1 ];then\
		go build -ldflags "-w -s \
		-X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=${version}" \
		-o sbin/curve $(PWD)/cmd/curvecli/main.go;\
	else\
		go build -gcflags "-N -l" -ldflags \
		"-X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=${version}" \
		-o sbin/curve $(PWD)/cmd/curvecli/main.go;\
	fi

init: proto
	go mod init github.com/opencurve/curve/tools-v2
	go mod tidy

proto:
	@bash mk-proto.sh

install_grpc_protobuf:
	# install protoc (https://github.com/protocolbuffers/protobuf/releases/tag/v21.8)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

clean:
	rm -rf sbin
	rm -rf proto/proto/*
	rm -rf proto/curvefs/*
	rm -rf go.mod
	rm -rf go.sum
