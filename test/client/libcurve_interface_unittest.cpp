/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:16:52 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>
#include <iostream>
#include <thread>   //NOLINT
#include <chrono>      // NOLINT
#include <condition_variable>  // NOLINT
#include <mutex>   // NOLINT

#include "include/client/libcurve.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"

using curve::client::ChunkServerAddr;
using curve::client::EndPoint;

extern std::string configpath;

bool writeflag = false;
bool readflag = false;
std::mutex writeinterfacemtx;
std::condition_variable writeinterfacecv;
std::mutex interfacemtx;
std::condition_variable interfacecv;

DECLARE_uint64(test_disk_size);
void writecallbacktest(CurveAioContext* context) {
    writeflag = true;
    writeinterfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
}
void readcallbacktest(CurveAioContext* context) {
    readflag = true;
    interfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
}

TEST(TestLibcurveInterface, InterfaceTest) {
    ASSERT_EQ(0, Init(configpath.c_str()));
    std::string filename = "/1_userinfo_";

    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    memcpy(userinfo.password, "", 256);

    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 8200, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    // libcurve file operation
    int temp = Create(filename.c_str(), &userinfo, FLAGS_test_disk_size);

    int fd = Open(filename.c_str(), &userinfo);

    ASSERT_NE(fd, -1);

    char* buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = writecallbacktest;

    AioWrite(fd, &writeaioctx);
    {
        std::unique_lock<std::mutex> lk(writeinterfacemtx);
        writeinterfacecv.wait(lk, []()->bool{return writeflag;});
    }
    writeflag = false;
    AioWrite(fd, &writeaioctx);
    {
        std::unique_lock<std::mutex> lk(writeinterfacemtx);
        writeinterfacecv.wait(lk, []()->bool{return writeflag;});
    }
    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;
    AioRead(fd, &readaioctx);
    {
        std::unique_lock<std::mutex> lk(interfacemtx);
        interfacecv.wait(lk, []()->bool{return readflag;});
    }

    for (int i = 0; i < 1024; i++) {
        ASSERT_EQ(readbuffer[i], 'a');
        ASSERT_EQ(readbuffer[i +  1024], 'b');
        ASSERT_EQ(readbuffer[i +  2 * 1024], 'c');
        ASSERT_EQ(readbuffer[i +  3 * 1024], 'd');
        ASSERT_EQ(readbuffer[i +  4 * 1024], 'e');
        ASSERT_EQ(readbuffer[i +  5 * 1024], 'f');
        ASSERT_EQ(readbuffer[i +  6 * 1024], 'g');
        ASSERT_EQ(readbuffer[i +  7 * 1024], 'h');
    }

    mds.EnableNetUnstable(400);
    int count = 0;
    while (count < 20) {
        uint64_t offset = 0;
        uint64_t length = 8 * 1024;

        memset(buffer, 'i', 1024);
        memset(buffer + 1024, 'j', 1024);
        memset(buffer + 2 * 1024, 'k', 1024);
        memset(buffer + 3 * 1024, 'l', 1024);
        memset(buffer + 4 * 1024, 'm', 1024);
        memset(buffer + 5 * 1024, 'n', 1024);
        memset(buffer + 6 * 1024, 'o', 1024);
        memset(buffer + 7 * 1024, 'p', 1024);

        ASSERT_EQ(length, Write(fd, buffer, offset, length));
        ASSERT_EQ(length, Read(fd, readbuffer, offset, length));

        for (int i = 0; i < 1024; i++) {
            ASSERT_EQ(readbuffer[i], 'i');
            ASSERT_EQ(readbuffer[i +  1024], 'j');
            ASSERT_EQ(readbuffer[i +  2 * 1024], 'k');
            ASSERT_EQ(readbuffer[i +  3 * 1024], 'l');
            ASSERT_EQ(readbuffer[i +  4 * 1024], 'm');
            ASSERT_EQ(readbuffer[i +  5 * 1024], 'n');
            ASSERT_EQ(readbuffer[i +  6 * 1024], 'o');
            ASSERT_EQ(readbuffer[i +  7 * 1024], 'p');
        }
        count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }

    /**
     * the disk is faked, the size is just = 10 * 1024 * 1024 * 1024.
     * when the offset pass the boundary, it will return failed.
     */ 
    off_t off = 10 * 1024 * 1024 * 1024ul;
    uint64_t len = 8 * 1024;

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Write(fd, buffer, off, len));
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Read(fd, readbuffer, off, len));

    off_t off1 = 1 * 1024 * 1024 * 1024ul - 8 * 1024;
    uint64_t len1 = 8 * 1024;

    LOG(ERROR) << "normal read write！";
    ASSERT_EQ(len, Write(fd, buffer, off1, len1));
    ASSERT_EQ(len, Read(fd, readbuffer, off1, len1));
    Close(fd);
    mds.UnInitialize();
    delete[] buffer;
    delete[] readbuffer;
    UnInit();
}

TEST(TestLibcurveInterface, InterfaceExceptionTest) {
    ASSERT_EQ(0, Init(configpath.c_str()));
    // open not create file
    std::string filename = "/1_userinfo_";

    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    memcpy(userinfo.password, "", 256);

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Open(filename.c_str(), &userinfo));

    char* buffer = new char[8 * 1024];
    memset(buffer, 'a', 8*1024);

    // not aligned test
    CurveAioContext ctx;
    ctx.buf = buffer;
    ctx.offset = 1;
    ctx.length = 7 * 1024;
    ctx.cb = writecallbacktest;
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, AioWrite(1234, &ctx));
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, AioRead(1234, &ctx));
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, Write(1234, buffer, 1, 4096));
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, Read(1234, buffer, 4096 , 123));

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = writecallbacktest;

    // aiowrite not opened file
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, AioWrite(1234, &writeaioctx));

    // aioread not opened file
    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, AioRead(1234, &readaioctx));

    uint64_t offset = 0;
    uint64_t length = 8 * 1024;

    // write not opened file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED,
                Write(1234, buffer, offset, length));
    // read not opened file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Read(1234,
                readbuffer, offset, length));

    delete[] buffer;
    delete[] readbuffer;
    UnInit();
}
