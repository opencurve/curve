
  
.PHONY: build

release?= 0
version?= "unknown"

build:
	protoc --go_out=proto --proto_path=.. ../curvefs/proto/*.proto
	protoc --go-grpc_out=proto --proto_path=.. ../curvefs/proto/*.proto
	@if [ ${release} = 1 ];then\
		go build -ldflags "-w -s \
		-X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=${version}" \
		-o sbin/curve $(PWD)/cmd/curvecli/main.go;\
	else\
		go build -ldflags \
		"-X github.com/opencurve/curve/tools-v2/pkg/cli/command/version.Version=${version}" \
		-o sbin/curve $(PWD)/cmd/curvecli/main.go;\
	fi

init: clean
	go mod init github.com/opencurve/curve/tools-v2
	go mod tidy

clean:
	rm -rf sbin
	rm -rf proto/*
	rm -rf go.mod
	rm -rf go.sum
