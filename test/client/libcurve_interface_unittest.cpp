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
#include <chrono>   //NOLINT

#include "include/client/libcurve.h"
#include "src/client/session.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"

DECLARE_uint64(test_disk_size);
void callbacktest(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->err;
}

TEST(TestLibcurveInterface, InterfaceTest) {
    std::string filename = "./test.txt";

    /*** init mds service ***/
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartService();
    mds.CreateCopysetNode();

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    /**** libcurve file operation ****/
    CreateFile(filename.c_str(), FLAGS_test_disk_size);

    int fd = Open(filename.c_str());

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
    writeaioctx.cb = callbacktest;

    AioWrite(fd, &writeaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    AioWrite(fd, &writeaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = callbacktest;
    AioRead(fd, &readaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

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

    ASSERT_EQ(8 * 1024, Write(fd, buffer, offset, length));
    ASSERT_EQ(8 * 1024, Read(fd, readbuffer, offset, length));

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

    /**
     * the disk is faked, the size is just = 10 * 1024 * 1024 * 1024.
     * when the offset pass the boundary, it will return failed.
     */ 
    off_t off = 10 * 1024 * 1024 * 1024ul;
    uint64_t len = 8 * 1024;

    ASSERT_EQ(-1, Write(fd, buffer, off, len));
    ASSERT_EQ(-1, Read(fd, readbuffer, off, len));

    off_t off1 = 10 * 1024 * 1024 * 1024ul - 8 * 1024;
    uint64_t len1 = 8 * 1024;

    ASSERT_EQ(8 * 1024, Write(fd, buffer, off1, len1));
    ASSERT_EQ(8 * 1024, Read(fd, readbuffer, off1, len1));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    mds.UnInitialize();
    delete buffer;
    delete readbuffer;
    Close(fd);
    UnInit();
}
