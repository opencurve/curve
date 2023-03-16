# Copyright (C) 2022 Jingli Chen (Wine93), NetEase Inc.

.PHONY: build init proto clean

# go env
GOPROXY     :=https://goproxy.cn,direct
GOOS        := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH      := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
CGO_LDFLAGS := "-static"
CC          := musl-gcc

GOENV := GO111MODULE=on
GOENV += GOPROXY=$(GOPROXY)
GOENV += CC=$(CC)
GOENV += CGO_ENABLED=1 CGO_LDFLAGS=$(CGO_LDFLAGS)
GOENV += GOOS=$(GOOS) GOARCH=$(GOARCH)

# go
GO := go

# output
OUTPUT := sbin/curve
DAEMON_OUTPUT := sbin/daemon

# version
VERSION_FLAG := -X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=$(version)

# build flags
CGO_BUILD_LDFLAGS := -s -w -linkmode external
CGO_BUILD_LDFLAGS += -extldflags "-static -fpic"
CGO_BUILD_FLAG += -ldflags '$(CGO_BUILD_LDFLAGS) $(VERSION_FLAG)'

BUILD_FLAGS := -a
BUILD_FLAGS += -trimpath
BUILD_FLAGS += $(CGO_BUILD_FLAG)
BUILD_FLAGS += $(EXTRA_FLAGS)

# debug flags
GCFLAGS := "all=-N -l"

CGO_DEBUG_LDFLAGS := -linkmode external
CGO_DEBUG_LDFLAGS += -extldflags "-static -fpic"
CGO_DEBUG_FLAG += -ldflags '$(CGO_DEBUG_LDFLAGS) $(VERSION_FLAG)'

DEBUG_FLAGS := -gcflags=$(GCFLAGS)
DEBUG_FLAGS += $(CGO_DEBUG_FLAG)

# packages
PACKAGES := $(PWD)/cmd/curve/main.go
DAEMON_PACKAGES := $(PWD)/cmd/daemon/main.go

build: proto
	$(GOENV) $(GO) build -o $(OUTPUT) $(BUILD_FLAGS) $(PACKAGES)
	$(GOENV) $(GO) build -o $(DAEMON_OUTPUT) $(BUILD_FLAGS) $(DAEMON_PACKAGES)

debug: proto
	$(GOENV) $(GO) build -o $(OUTPUT) $(DEBUG_FLAGS) $(PACKAGES)
	$(GOENV) $(GO) build -o $(DAEMON_OUTPUT) $(DEBUG_FLAGS) $(DAEMON_PACKAGES)

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
