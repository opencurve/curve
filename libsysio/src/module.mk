if WITH_THREAD_MODEL_POSIX
THREAD_MODEL_POSIX_SRCS = src/smp_posix.c
else
THREAD_MODEL_POSIX_SRCS =
endif

if WITH_TRACING
TRACING_SRCS = src/tracing.c
else
TRACING_SRCS =
endif

if WITH_STATVFS
STATVFS_SRCS = src/statvfs.c
else
STATVFS_SRCS =
endif

SRCDIR_SRCS = src/access.c src/chdir.c src/chmod.c \
	src/chown.c src/dev.c src/dup.c src/fcntl.c \
	src/file.c src/fs.c src/fsync.c \
	src/getdirentries.c src/init.c src/inode.c \
	src/ioctl.c src/ioctx.c src/iowait.c \
	src/link.c src/lseek.c src/mkdir.c \
	src/mknod.c src/mount.c src/namei.c \
	src/open.c src/rw.c src/reconcile.c src/rename.c \
	src/rmdir.c $(THREAD_MODEL_POSIX_SRCS) src/stat.c $(STATVFS_SRCS) \
	src/stddir.c src/readdir.c src/readdir64.c \
	src/symlink.c src/readlink.c \
	src/truncate.c src/unlink.c src/utime.c \
	$(TRACING_SRCS) src/cprintf.c src/tree.c

SRCDIR_EXTRA = src/module.mk
