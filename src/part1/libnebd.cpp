/*
 * Project: nebd
 * File Created: 2019-08-07
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#include "src/part1/libnebd.h"
#include "src/part1/libnebd_file.h"

extern "C" {
bool g_inited = false;

// Note: 配置文件路径是否有上层传下来比较合适，评估是否要修改
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

int nebd_lib_uninit() {
    if (g_inited) {
        Uninit4Nebd();
        g_inited = false;
    }

    return 0;
}

int nebd_lib_open(const char* filename) {
    return Open4Nebd(filename);
}

int nebd_lib_close(int fd) {
    return Close4Nebd(fd);
}

int nebd_lib_pread(int fd, void* buf, off_t offset, size_t length) {
    // not support sync read
    return -1;
}

int nebd_lib_pwrite(int fd, const void* buf, off_t offset, size_t length) {
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
    return 0;
}

int64_t nebd_lib_filesize(int fd) {
    return GetFileSize4Nebd(fd);
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

}  // extern "C"
