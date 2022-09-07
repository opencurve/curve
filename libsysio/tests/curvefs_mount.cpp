#include <stdio.h>
#include <errno.h>
#include <string.h>

#include <sys/queue.h>
#include <sys/mount.h>


#include "fs_curve.h"
#include "fuse_lowlevel.h"
#include "fuse_i.h"

extern "C" {
#include "mount.h"
}

struct mount_opts {
	int allow_other;
	int flags;
	int auto_unmount;
	int blkdev;
	char *fsname;
	char *subtype;
	char *subtype_opt;
	char *mtab_opts;
	char *fusermount_opts;
	char *kernel_opts;
	unsigned max_read;
};

struct mount_flags {
	const char *opt;
	unsigned long flag;
	int on;
};

static const struct mount_flags mount_flags[] = {
	{"rw",	    MS_RDONLY,	    0},
	{"ro",	    MS_RDONLY,	    1},
	{"suid",    MS_NOSUID,	    0},
	{"nosuid",  MS_NOSUID,	    1},
	{"dev",	    MS_NODEV,	    0},
	{"nodev",   MS_NODEV,	    1},
	{"exec",    MS_NOEXEC,	    0},
	{"noexec",  MS_NOEXEC,	    1},
	{"async",   MS_SYNCHRONOUS, 0},
	{"sync",    MS_SYNCHRONOUS, 1},
	{"atime",   MS_NOATIME,	    0},
	{"noatime", MS_NOATIME,	    1},
#ifndef __NetBSD__
	{"dirsync", MS_DIRSYNC,	    1},
#endif
	{NULL,	    0,		    0}
};

static int get_mnt_flag_opts(char **mnt_optsp, int flags)
{
	int i;

	if (!(flags & MS_RDONLY) && fuse_opt_add_opt(mnt_optsp, "rw") == -1)
		return -1;

	for (i = 0; mount_flags[i].opt != NULL; i++) {
		if (mount_flags[i].on && (flags & mount_flags[i].flag) &&
		    fuse_opt_add_opt(mnt_optsp, mount_flags[i].opt) == -1)
			return -1;
	}
	return 0;
}

static int curvefs_mount_sys(const char *mnt, struct mount_opts *mo) {
	char tmp[128];
	const char *devname = "/dev/fuse";
	char *source = NULL;
	char *type = NULL;
	struct stat stbuf;
	int res = 0;

	if (!mnt) {
		fuse_log(FUSE_LOG_ERR, "fuse: missing mountpoint parameter\n");
		return -1;
	}

	res = stat(mnt, &stbuf);
	if (res == -1) {
		fuse_log(FUSE_LOG_ERR, "fuse: failed to access mountpoint %s: %s\n",
			mnt, strerror(errno));
		return -1;
	}
	snprintf(tmp, sizeof(tmp),  "rootmode=%o,user_id=%u,group_id=%u",
		stbuf.st_mode & S_IFMT, getuid(), getgid());

	res = fuse_opt_add_opt(&mo->kernel_opts, tmp);
	if (res == -1)
		goto out_close;

	source =(char *) malloc((mo->fsname ? strlen(mo->fsname) : 0) +
			(mo->subtype ? strlen(mo->subtype) : 0) +
			strlen(devname) + 32);

	type =(char *) malloc((mo->subtype ? strlen(mo->subtype) : 0) + 32);
	if (!type || !source) {
		fuse_log(FUSE_LOG_ERR, "fuse: failed to allocate memory\n");
		goto out_close;
	}

	strcpy(type, mo->blkdev ? "fuseblk" : "curvefs");
	#if 0
	if (mo->subtype) {
		strcat(type, ".");
		strcat(type, mo->subtype);
	}
	
	strcpy(source,
	       mo->fsname ? mo->fsname : (mo->subtype ? mo->subtype : devname));
	#endif

	strcpy(source, "/");

	fuse_log(FUSE_LOG_ERR, "curvefs mount the source %s the mnt %s the type %s\n", source, mnt, type);
	res = _sysio_mount_root(source, type, mo->flags, mo->kernel_opts);
#if 0
	res = SYSIO_INTERFACE_NAME(mount)(source, mnt, type, mo->flags, mo->kernel_opts);
#endif
	
	free(type);
	free(source);

	return res;

out_close:
	free(type);
	free(source);
	return res;
}

int curvefs_session_mount(struct fuse_session *se, const char *mountpoint) {
	int res = -1;
	char *mnt_opts = NULL;

	fuse_log(FUSE_LOG_ERR, "se fsname %s, subtype %s, subtype_opt %s, opts %s, kernelopt %s\n", se->mo->fsname, se->mo->subtype, 
		se->mo->subtype_opt, se->mo->fusermount_opts, se->mo->kernel_opts);

	if (get_mnt_flag_opts(&mnt_opts, se->mo->flags) == -1)
		goto out;

	if (se->mo->kernel_opts && fuse_opt_add_opt(&mnt_opts, se->mo->kernel_opts) == -1)
		goto out;

	res = curvefs_mount_sys(mountpoint, se->mo);

out:
	free(mnt_opts);
	return res;
}
