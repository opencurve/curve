# Copyright (C) 2023 Jingli Chen (Wine93), NetEase Inc.

# FIMXE: use makefile ^@
# NOTE: now we only support run make in playground

so_path?= /curve/bazel-bin/curvefs/sdk/libcurvefs
hrd_opt?= -I/curve
build_opt?= -L$(so_path)
link_opt?= -Wl,-rpath=$(so_path) -Wall
flags?= $(hrd_opt) $(build_opt) $(link_opt)

libcurvefs:
	mkdir -p bin
	bazel build --compilation_mode=dbg --config=gcc7-later //curvefs/sdk/libcurvefs:curvefs

mkdir: libcurvefs
	gcc ${flags} mkdir.c -o bin/mkdir -lcurvefs

rmdir: libcurvefs
	gcc ${flags} rmdir.c -o bin/rmdir -lcurvefs

ls: libcurvefs
	gcc ${flags} ls.c -o bin/ls -lcurvefs

touch: libcurvefs
	gcc ${flags} touch.c -o bin/touch -lcurvefs

read: libcurvefs
	gcc ${flags} read.c -o bin/read -lcurvefs

write: libcurvefs
	gcc ${flags} write.c -o bin/write -lcurvefs

unlink: libcurvefs
	gcc ${flags} unlink.c -o bin/unlink -lcurvefs

append: libcurvefs
	gcc ${flags} append.c -o bin/append -lcurvefs

rename: libcurvefs
	gcc ${flags} rename.c -o bin/rename -lcurvefs

stat: libcurvefs
	gcc ${flags} stat.c -o bin/stat -lcurvefs

statfs: libcurvefs
	gcc ${flags} statfs.c -o bin/statfs -lcurvefs

fstat: libcurvefs
	gcc ${flags} fstat.c -o bin/fstat -lcurvefs

chmod: libcurvefs
	gcc ${flags} chmod.c -o bin/chmod -lcurvefs

all: mkdir rmdir ls touch read write unlink append rename stat statfs fstat chmod
