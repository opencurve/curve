/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


/*
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/curve_fuse_op.h"
#include "curvefs/src/client/fuse_common.h"

static const struct fuse_lowlevel_ops curve_ll_oper = {
    .init       = FuseOpInit,
    .destroy    = FuseOpDestroy,
    .lookup     = FuseOpLookup,
    .rename     = FuseOpRename,
    .write      = FuseOpWrite,
    .read       = FuseOpRead,
    .open       = FuseOpOpen,
    .create     = FuseOpCreate,
    .mknod      = FuseOpMkNod,
    .mkdir      = FuseOpMkDir,
    .unlink     = FuseOpUnlink,
    .rmdir      = FuseOpRmDir,
    .opendir    = FuseOpOpenDir,
    .readdir    = FuseOpReadDir,
    .getattr    = FuseOpGetAttr,
    .setattr    = FuseOpSetAttr,
    .getxattr   = FuseOpGetXattr,
    .listxattr  = FuseOpListXattr,
    .symlink    = FuseOpSymlink,
    .link       = FuseOpLink,
    .readlink   = FuseOpReadLink,
    .release    = FuseOpRelease,
    .fsync      = FuseOpFsync,
    .releasedir = FuseOpReleaseDir,
    .flush      = FuseOpFlush,
    .bmap       = FuseOpBmap,
    .statfs     = FuseOpStatFs,
};

void print_option_help(const char* o, const char* msg) {
    printf("    -o %-20s%s\n", o, msg);
}

void extra_options_help() {
    printf("\nExtra options:\n");
    print_option_help("fsname", "[required] name of filesystem to be mounted");
    print_option_help(
        "fstype", "[required] type of filesystem to be mounted (s3/volume)");
    print_option_help("conf", "[required] path of config file");
    print_option_help("mdsAddr", "mdsAddr of curvefs cluster");
}

int main(int argc, char *argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_session *se;
    struct fuse_cmdline_opts opts;
    struct fuse_loop_config config;
    struct MountOption mOpts = {0};
    int ret = -1;

    if (fuse_parse_cmdline(&args, &opts) != 0)
        return 1;
    if (opts.show_help) {
        printf(
            "usage: %s -o conf=/etc/curvefs/client.conf -o fsname=testfs \\\n"
            "       -o fstype=s3 [-o mdsaddr=1.1.1.1] [OPTIONS] <mountpoint>\n",
            argv[0]);
        printf("Fuse Options:\n");
        fuse_cmdline_help();
        fuse_lowlevel_help();
        extra_options_help();
        ret = 0;
        goto err_out1;
    } else if (opts.show_version) {
        printf("FUSE library version %s\n", fuse_pkgversion());
        fuse_lowlevel_version();
        ret = 0;
        goto err_out1;
    }

    if (opts.mountpoint == NULL) {
        printf("required option is missing: mountpoint\n");
        ret = 1;
        goto err_out1;
    }

    if (fuse_opt_parse(&args, &mOpts, mount_opts, NULL)== -1)
        return 1;

    mOpts.mountPoint = opts.mountpoint;

    if (mOpts.conf == NULL || mOpts.fsName == NULL || mOpts.fsType == NULL) {
        printf(
            "one of required options is missing. conf, fsname, fstype are "
            "required.\n");
        ret = 1;
        goto err_out1;
    }

    printf("Begin to mount fs %s to %s\n", mOpts.fsName, mOpts.mountPoint);

    se = fuse_session_new(&args, &curve_ll_oper,
                  sizeof(curve_ll_oper), &mOpts);
    if (se == NULL)
        goto err_out1;

    if (fuse_set_signal_handlers(se) != 0)
        goto err_out2;

    if (fuse_session_mount(se, opts.mountpoint) != 0)
        goto err_out3;

    fuse_daemonize(opts.foreground);

    if (InitGlog(mOpts.conf, argv[0]) < 0) {
        printf("Init glog failed, confpath = %s\n", mOpts.conf);
    }

    ret = InitFuseClient(mOpts.conf, mOpts.fsName, mOpts.fsType, mOpts.mdsAddr);
    if (ret < 0) {
        printf("init fuse client fail, conf =%s\n", mOpts.conf);
        goto err_out4;
    }

    printf("fuse start loop, singlethread = %d, max_idle_threads = %d\n",
        opts.singlethread, opts.max_idle_threads);

    /* Block until ctrl+c or fusermount -u */
    if (opts.singlethread) {
        ret = fuse_session_loop(se);
    } else {
        config.clone_fd = opts.clone_fd;
        config.max_idle_threads = opts.max_idle_threads;
        ret = fuse_session_loop_mt(se, &config);
    }

err_out4:
    fuse_session_unmount(se);
err_out3:
    fuse_remove_signal_handlers(se);
err_out2:
    fuse_session_destroy(se);
err_out1:
    UnInitFuseClient();
    free(opts.mountpoint);
    fuse_opt_free_args(&args);

    return ret ? 1 : 0;
}
