# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: build install deploy core config topo start stop reload status clean mount umount

prefix?= "$(PWD)/devops/projects"
release?= 0
only?= "*"
hosts?= "*"
tag?= "curvefs:unknown"
os?= "debian9"

define deploy_begin
	@bash util/deploy.sh begin
endef

define deploy_end
	@bash util/deploy.sh end
endef

define only_service
	$(call deploy_begin)
	@bash util/deploy.sh --hosts=$(hosts) --only="etcd" --tags=$(1)
	@bash util/deploy.sh --hosts=$(hosts) --only="mds" --tags=$(1)
	@bash util/deploy.sh --hosts=$(hosts) --only="metaserver" --tags=$(1)
	$(call deploy_end)
endef

define only_specify
	$(call deploy_begin)
	@bash util/deploy.sh --hosts=$(hosts) --only=$(1) --tags=$(2)
	$(call deploy_end)
endef

build:
	@bash util/build.sh --only=$(only) --release=$(release) --os=$(os)

install:
	@bash util/install.sh --prefix=$(prefix) --only=$(only)

image:
	@bash util/image.sh $(tag) $(os)

deploy:
	$(call deploy_begin)
	@bash util/deploy.sh --hosts="etcd" --only="etcd" --tags="core,config,start"
	@bash util/deploy.sh --hosts="mds" --only="mds" --tags="core,config,start"
	@bash util/deploy.sh --hosts="metaserver" --only="metaserver" --tags="core,config,start"
	@bash util/deploy.sh --hosts="tools" --only="tools" --tags="core,config,topo"
	@bash util/deploy.sh --hosts="client" --only="client" --tags="core,config"
	$(call deploy_end)

core:
	$(call only_specify,$(only),core)

config:
	$(call only_specify,$(only),config)

topo:
	$(call only_specify,tools,topo)

debug:
	$(call only_specify,$(only),debug)

start:
ifeq ($(only), "*")
	$(call only_service,start)
else
	$(call only_specify,$(only),start)
endif

stop:
ifeq ($(only), "*")
	$(call only_service,stop)
else
	$(call only_specify,$(only),stop)
endif

reload:
ifeq ($(only), "*")
	$(call only_service,restart)
else
	$(call only_specify,$(only),restart)
endif

status:
ifeq ($(only), "*")
	$(call only_service,status)
else
	$(call only_specify,$(only),status)
endif

clean:
	$(call only_specify,$(only),clean)

mount:
	$(call only_specify,client,mount)

umount:
	$(call only_specify,client,umount)
