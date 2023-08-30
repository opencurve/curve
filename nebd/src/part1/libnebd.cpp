/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 */

#include "nebd/src/part1/libnebd.h"
#include "nebd/src/part1/libnebd_file.h"

extern "C" {
bool g_inited = false;

//Note: It is more appropriate to pass down the configuration file path from the upper level, and evaluate whether it needs to be modified
const char* confpath = "/etc/nebd/nebd-client.conf";
int nebd_lib_init() {
    if (g_inited) {
        return 0;
    }

    int ret = Init4Nebd(confpath);
    if (ret != 0) {
        return ret;
    }

    g_inited = true;

    return ret;
}

int nebd_lib_init_with_conf(const char* confPath) {
    if (g_inited) {
        return 0;
    }

    int ret = Init4Nebd(confPath);
    if (ret != 0) {
        return ret;
    }

    g_inited = true;

    return ret;
}

int nebd_lib_uninit() {
    if (g_inited) {
        Uninit4Nebd();
        g_inited = false;
    }

    return 0;
}

int nebd_lib_open(const char* filename) {
    return Open4Nebd(filename, nullptr);
}

int nebd_lib_open_with_flags(const char* filename, const NebdOpenFlags* flags) {
    return Open4Nebd(filename, flags);
}

int nebd_lib_close(int fd) {
    return Close4Nebd(fd);
}

int nebd_lib_pread(int fd, void* buf, off_t offset, size_t length) {
    (void)fd;
    (void)buf;
    (void)offset;
    (void)length;
    // not support sync read
    return -1;
}

int nebd_lib_pwrite(int fd, const void* buf, off_t offset, size_t length) {
    (void)fd;
    (void)buf;
    (void)offset;
    (void)length;
    // not support sync write
    return -1;
}

int nebd_lib_discard(int fd, NebdClientAioContext* context) {
    return Discard4Nebd(fd, context);
}

int nebd_lib_aio_pread(int fd, NebdClientAioContext* context) {
    return AioRead4Nebd(fd, context);
}

int nebd_lib_aio_pwrite(int fd, NebdClientAioContext* context) {
    return AioWrite4Nebd(fd, context);
}

int nebd_lib_sync(int fd) {
    (void)fd;
    return 0;
}

int64_t nebd_lib_filesize(int fd) {
    return GetFileSize4Nebd(fd);
}

int64_t nebd_lib_blocksize(int fd) {
    return GetBlockSize4Nebd(fd);
}

int nebd_lib_resize(int fd, int64_t size) {
    return Extend4Nebd(fd, size);
}

int nebd_lib_flush(int fd, NebdClientAioContext* context) {
    return Flush4Nebd(fd, context);
}

int64_t nebd_lib_getinfo(int fd) {
    return GetInfo4Nebd(fd);
}

int nebd_lib_invalidcache(int fd) {
    return InvalidCache4Nebd(fd);
}

void nebd_lib_init_open_flags(NebdOpenFlags* flags) {
    flags->exclusive = 1;
}

}  // extern "C"
