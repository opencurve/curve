#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

SHELL=/bin/bash

pwd := $(shell pwd)


# Download and build libetcdclient only when necessary, i.e.,
# when dependent code is changed.
# Typically, this target should always be used to avoid unnecessary
# rebuilds.
build: libetcdclient.so libetcdclient.h expose-session-for-election.patch etcdclient.go objectManager.go

libetcdclient.so libetcdclient.h: # do not use `clean' and `all' targets here as they will always be built
	$(MAKE) clean all

# Download and build libetcdclient, regardless of whether the code
# is changed or not.
all: install-go install-etcdclient libetcdclient


install-go:
	mkdir -p $(pwd)/tmp
	cd $(pwd)/tmp && wget -N https://golang.google.cn/dl/go1.12.8.linux-amd64.tar.gz
	cd $(pwd)/tmp && tar zxvf go1.12.8.linux-amd64.tar.gz

install-etcdclient:
	mkdir -p $(pwd)/tmp/gosrc/src/go.etcd.io
	cd $(pwd)/tmp/gosrc/src/go.etcd.io && git clone --branch v3.4.0 --depth=1 https://gitee.com/mirrors/etcd
	cd $(pwd)/tmp/gosrc/src/go.etcd.io/etcd && cp $(pwd)/expose-session-for-election.patch . && patch -p1 < expose-session-for-election.patch

vendorpath := $(pwd)/tmp/gosrc/src/go.etcd.io/etcd/vendor
$(eval export GOROOT=${pwd}/tmp/go)
$(eval export GOPATH=${pwd}/tmp/gosrc)
libetcdclient:
	cp etcdclient.go ${pwd}/tmp/gosrc/src && cp objectManager.go ${pwd}/tmp/gosrc/src
	mv $(vendorpath)/google.golang.org $(pwd)/tmp/gosrc/src/
	mv $(vendorpath)/golang.org $(pwd)/tmp/gosrc/src/
	mv $(vendorpath)/github.com $(pwd)/tmp/gosrc/src/
	$(pwd)/tmp/go/bin/go build -buildmode=c-shared -o libetcdclient.so ${pwd}/tmp/gosrc/src/etcdclient.go ${pwd}/tmp/gosrc/src/objectManager.go

clean:
	rm -fr $(pwd)/tmp
	rm -fr $(pwd)/libetcdclient.so
	rm -fr $(pwd)/libetcdclient.h
