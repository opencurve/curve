/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/11/23  Wenyu Zhou   Initial version
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>

#define CBD_BACKEND_FAKE

#include "src/client/libcbd.h"
#include "include/client/libcurve.h"

void LibcbdExt4TestCallback(CurveAioContext* context) {
    context->op = LIBCURVE_OP_MAX;
}

TEST(TestLibcbdExt4, InitTest) {
    int ret;
    CurveOptions opt;

    memset(&opt, 0, sizeof(opt));

    opt.datahome = ".";
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);

    // reinit again
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, 0);
}

TEST(TestLibcbdExt4, ReadWriteTest) {
    int ret;
    int fd;
    int i;
#define BUFSIZE 4 * 1024
#define FILESIZE 1 * 1024 * 1024
    char buf[BUFSIZE];
    std::string filename = "test.img";
    CurveOptions opt;

    memset(&opt, 0, sizeof(opt));
    memset(buf, 'a', BUFSIZE);

    opt.datahome = ".";
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);

    fd = cbd_lib_open(filename.c_str());
    ASSERT_GE(fd, 0);

    ret = fallocate(fd, 0, 0, FILESIZE);
    ASSERT_GE(fd, 0);

    ret = cbd_lib_filesize(filename.c_str());
    ASSERT_EQ(ret, FILESIZE);

    ret = cbd_lib_pwrite(fd, buf, 0, BUFSIZE);
    ASSERT_EQ(ret, BUFSIZE);

    ret = cbd_lib_sync(fd);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_pread(fd, buf, 0, BUFSIZE);
    ASSERT_EQ(ret, BUFSIZE);

    for (i = 0; i < BUFSIZE; i++) {
        if (buf[i] != 'a') {
            break;
        }
    }
    ASSERT_EQ(i, BUFSIZE);

    ret = cbd_lib_close(fd);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, 0);
}

TEST(TestLibcbdExt4, AioReadWriteTest) {
    int ret;
    int fd;
    int i;
#define BUFSIZE 4 * 1024
#define FILESIZE 1 * 1024 * 1024
    char buf[BUFSIZE];
    std::string filename = "test.img";
    CurveOptions opt;
    CurveAioContext aioCtx;

    aioCtx.buf = buf;
    aioCtx.offset = 0;
    aioCtx.length = BUFSIZE;
    aioCtx.cb = LibcbdExt4TestCallback;

    memset(&opt, 0, sizeof(opt));
    memset(buf, 'a', BUFSIZE);

    opt.datahome = ".";
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);

    fd = cbd_lib_open(filename.c_str());
    ASSERT_GE(fd, 0);

    ret = fallocate(fd, 0, 0, FILESIZE);
    ASSERT_GE(fd, 0);

    ret = cbd_lib_filesize(filename.c_str());
    ASSERT_EQ(ret, FILESIZE);

    aioCtx.op = LIBCURVE_OP_WRITE;
    ret = cbd_lib_aio_pwrite(fd, &aioCtx);
    ASSERT_EQ(ret, 0);

    while (aioCtx.op == LIBCURVE_OP_WRITE) {
        usleep(10 * 1000);
    }

    ret = cbd_lib_sync(fd);
    ASSERT_EQ(ret, 0);

    aioCtx.op = LIBCURVE_OP_READ;
    ret = cbd_lib_aio_pread(fd, &aioCtx);
    ASSERT_EQ(ret, 0);

    while (aioCtx.op == LIBCURVE_OP_READ) {
        usleep(10 * 1000);
    }

    for (i = 0; i < BUFSIZE; i++) {
        if (buf[i] != 'a') {
            break;
        }
    }
    ASSERT_EQ(i, BUFSIZE);

    ret = cbd_lib_close(fd);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, 0);
}
