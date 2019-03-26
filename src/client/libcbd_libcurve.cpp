/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/10/10  Wenyu Zhou   Initial version
 */

#include "src/client/libcbd.h"
#include "src/client/config_info.h"
#include "include/client/libcurve_qemu.h"

extern "C" {

CurveOptions g_cbd_libcurve_options = {0};

int cbd_libcurve_init(const CurveOptions* options) {
    int ret;
    if (g_cbd_libcurve_options.inited) {
        return 0;
    }
    g_cbd_libcurve_options.conf = options->conf;
    ret = Init(options->conf);
    if (ret != 0) {
        return ret;
    }
    g_cbd_libcurve_options.inited = true;

    return ret;
}

int cbd_libcurve_fini() {
    UnInit();
    g_cbd_libcurve_options.inited = false;

    return 0;
}

int cbd_libcurve_open(const char* filename) {
    int fd = -1;

    fd = Open(filename, 0, false);

    return fd;
}

int cbd_libcurve_close(int fd) {
    Close(fd);

    return 0;
}

int cbd_libcurve_pread(int fd, void* buf, off_t offset, size_t length) {
    return Read(fd, reinterpret_cast<char*>(buf), offset, length);
}

int cbd_libcurve_pwrite(int fd, const void* buf, off_t offset, size_t length) {
    return Write(fd, reinterpret_cast<const char*>(buf), offset, length);
}

int cbd_libcurve_aio_pread(int fd, CurveAioContext* context) {
    return AioRead(fd, context);
}

int cbd_libcurve_aio_pwrite(int fd, CurveAioContext* context) {
    return AioWrite(fd, context);
}

int cbd_libcurve_sync(int fd) {
    // Ignored as it always sync writes to chunkserver currently
    return 0;
}

int64_t cbd_libcurve_filesize(const char* filename) {
    int fd = cbd_libcurve_open(filename);
    if (fd < 0) {
        return -1;
    }
    struct FileStatInfo info;
    StatFs(fd, &info);
    cbd_libcurve_close(fd);
    return info.length;
}

}  // extern "C"
