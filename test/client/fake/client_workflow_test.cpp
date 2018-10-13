/*
 * Project: curve
 * File Created: Saturday, 13th October 2018 1:59:08 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>
#include <iostream>
#include <atomic>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT

#include "include/client/libcurve.h"
#include "src/client/session.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"

void callback(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->err;
}

int main(int argc, char ** argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    std::string filename = "/home/test.txt";
    uint64_t size = 1 * 1024 * 1024 * 1024;

    /*** init mds service ***/
    FakeMDS mds(filename, size);
    mds.Initialize();
    mds.StartService();
    mds.FakeCreateCopysetReturn();
    mds.CreateCopysetNode();

    /**** libcurve file operation ****/
    CreateFile(filename.c_str(), size);

    int fd = Open(filename.c_str());

    if (fd == -1) {
        LOG(FATAL) << "open file failed!";
        return -1;
    }

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
    writeaioctx.cb = callback;

    AioWrite(fd, &writeaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    AioWrite(fd, &writeaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = callback;
    AioRead(fd, &readaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (int i = 0; i < 1024; i++) {
        if (readbuffer[i] != 'a') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  1024] != 'b') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  2 * 1024] != 'c') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  3 * 1024] != 'd') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  4 * 1024] != 'e') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  5 * 1024] != 'f') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  6 * 1024] != 'g') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
        if (readbuffer[i +  7 * 1024] != 'h') {
            LOG(FATAL) << "read wrong data!";
            break;
        }
    }

    std::atomic<bool> stop(false);
    auto testfunc = [&]() {
        while (!stop.load()) {
            AioRead(fd, &readaioctx);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            for (int i = 0; i < 1024; i++) {
                if (readbuffer[i] != 'a') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  1024] != 'b') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  2 * 1024] != 'c') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  3 * 1024] != 'd') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  4 * 1024] != 'e') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  5 * 1024] != 'f') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  6 * 1024] != 'g') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
                if (readbuffer[i +  7 * 1024] != 'h') {
                    LOG(FATAL) << "read wrong data!";
                    break;
                }
            }
        }
    };

    std::thread t(testfunc);
    char c;
    std::cin >> c;

    stop.store(true);
    if (t.joinable()) {
        t.join();
    }

    mds.UnInitialize();
    delete buffer;
    delete readbuffer;
    Close(fd);
    return 0;
}
