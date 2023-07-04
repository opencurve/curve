/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-07-25
 * Author: Jingli Chen (Wine93)
 */

#include <string>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/client/fuse/fuse_ll.h"
#include "curvefs/src/client/fuse/fuse.h"

namespace curvefs {
namespace client {
namespace fuse {

const struct fuse_lowlevel_ops curve_fuse_ll_ops = {
    init : fuse_ll_init,
    destroy : fuse_ll_destroy,
    lookup : fuse_ll_lookup,
    forget : 0,
    getattr : fuse_ll_getattr,
    setattr : fuse_ll_setattr,
    readlink : fuse_ll_readlink,
    mknod : fuse_ll_mknod,
    mkdir : fuse_ll_mkdir,
    unlink : fuse_ll_unlink,
    rmdir : fuse_ll_rmdir,
    symlink : fuse_ll_symlink,
    rename : fuse_ll_rename,
    link : fuse_ll_link,
    open : fuse_ll_open,
    read : fuse_ll_read,
    write : fuse_ll_write,
    flush : fuse_ll_flush,
    release : fuse_ll_release,
    fsync : fuse_ll_fsync,
    opendir : fuse_ll_opendir,
    #if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
    readdir : 0,
    #else
    readdir : fuse_ll_readdir,
    #endif
    releasedir : fuse_ll_releasedir,
    fsyncdir : 0,
    statfs : fuse_ll_statfs,
    setxattr : fuse_ll_setxattr,
    getxattr : fuse_ll_getxattr,
    listxattr : fuse_ll_listxattr,
    removexattr : 0,
    access : 0,
    create : fuse_ll_create,
    getlk : 0,
    setlk : 0,
    bmap : fuse_ll_bmap,
    #if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
    ioctl : 0,
    poll : 0,
    #endif
    #if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 9)
    write_buf : 0,
    retrieve_reply : 0,
    forget_multi : 0,
    flock : 0,
    fallocate : 0,
    #endif
    #if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
    readdirplus : fuse_ll_readdirplus,
    #else
    readdirplus : 0,
    #endif
    #if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 4)
    copy_file_range : 0,
    #endif
    #if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 8)
    lseek : 0
    #endif
};

FuseService::FuseService(MountOption mount)
    : mount_(mount) {}

bool FuseService::Run(struct fuse_cmdline_opts opts) {
    // 1) new session
    auto client = std::make_shared_ptr<FuseClient>;
    struct fuse_session* session;
    session = fuse_session_new(&args, &curve_fuse_ll_ops,
                               sizeof(curve_fuse_ll_ops), &client);
    if (nullptr == session) {
        return false;
    }
    auto deferDestorySession = absl::MakeCleanup([&]() {
        fuse_session_destroy(session)
    });

    // 2) set signal handlers
    int rc = fuse_set_signal_handlers(session);
    if (rc != 0) {
        return false;
    }
    auto deferRemoveHandler = absl::MakeCleanup([&]() {
        fuse_remove_signal_handlers(session)
    });

    // 3) mount session
    std::string mountpoint = mount_.mountPoint;
    rc = fuse_session_mount(session, mountpoint.c_str());
    if (rc != 0) {
        return false;
    }
    auto deferUmount = absl::MakeCleanup([&]() {
        fuse_session_unmount(session);
    });

    // 4) daemonize
    fuse_daemonize(opts.foreground);

    // 5) new client and run it
    auto helper = Helper();
    auto yes = helper::NewClientForFuse(mount, &client);
    if (!yes) {
        return false;
    }
    auto deferUninitClient = absl::MakeCleanup([&]() {
        client->Fini();
        client->UnInit();
    });

    LOG(INFO) << "Fuse service running, singlethread = " << opts.singlethread
              << ", max_idle_threads = " << opts.max_idle_threads;

    // 6) block until ctrl+c or fusermount -u
    if (opts.singlethread) {
        rc = fuse_session_loop(session);
    } else {
        struct fuse_loop_config config;
        config.clone_fd = opts.clone_fd;
        config.max_idle_threads = opts.max_idle_threads;
        rc = fuse_session_loop_mt(session, &config);
    }

    return rc == 0;
}

}  // namespace fuse
}  // namespace client
}  // namespace curvefs
