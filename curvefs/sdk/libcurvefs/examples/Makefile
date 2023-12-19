# Copyright (C) 2024 Jingli Chen (Wine93), NetEase Inc.

root= $(shell echo $${PWD%/curvefs/sdk/libcurvefs/examples})
so_path?= $(root)/bazel-bin/curvefs/sdk/libcurvefs
hrd_opt?= -I/curve
build_opt?= -L$(so_path)
link_opt?= -Wl,-rpath=$(so_path) -Wall
flags?= $(hrd_opt) $(build_opt) $(link_opt)
targets= mkdir rmdir ls touch read write unlink append rename stat statfs fstat chmod

libcurvefs:
	mkdir -p bin
	bazel build \
        --config=gcc7-later \
        --compilation_mode=opt \
        --copt -g \
        --copt -DUNINSTALL_SIGSEGV=1 \
        --copt -DCLIENT_CONF_PATH="\"$(root)/curvefs/conf/client.conf\"" \
        //curvefs/sdk/libcurvefs:curvefs

$(targets): libcurvefs
	gcc ${flags} $@.c -o bin/$@ -lcurvefs

all: $(targets)
