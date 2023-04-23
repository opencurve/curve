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

#include "include/client/libcbd.h"
#include "include/client/libcurve.h"

void LibcbdExt4TestCallback(CurveAioContext* context) {
    context->op = LIBCURVE_OP_MAX;
}

TEST(TestLibcbdExt4, InitTest) {
    int ret;
    CurveOptions opt;

    memset(&opt, 0, sizeof(opt));

    opt.datahome = const_cast<char*>(".");
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

    opt.datahome = const_cast<char*>(".");
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

    opt.datahome = const_cast<char*>(".");
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

TEST(TestLibcbdExt4, IncreaseEpochTest) {
    int ret;
    CurveOptions opt;
    std::string filename = "test.img";

    memset(&opt, 0, sizeof(opt));

    opt.datahome = const_cast<char*>(".");
    ret = cbd_lib_init(&opt);
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_increase_epoch(filename.c_str());
    ASSERT_EQ(ret, 0);

    ret = cbd_lib_fini();
    ASSERT_EQ(ret, 0);
}


