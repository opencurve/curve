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

DECLARE_uint32(chunk_size);
DECLARE_uint64(test_disk_size);
DEFINE_uint32(io_time, 5, "Duration for I/O test");
DEFINE_bool(fake_mds, true, "create fake mds");
DEFINE_bool(verify_io, true, "verify read/write I/O getting done correctly");

void callback(CurveAioContext* context) {
    LOG(INFO) << "aio call back for op " << context->op
              << " here, errorcode = " << context->err;
}

int main(int argc, char ** argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    std::string filename = "data/test.txt";
    uint64_t size = FLAGS_test_disk_size;

    /*** init mds service ***/
    FakeMDS mds(filename, size);
    if (FLAGS_fake_mds) {
        mds.Initialize();
        mds.StartService();
        mds.CreateCopysetNode();
    }

    /**** libcurve file operation ****/
    CreateFile(filename.c_str(), size);

    sleep(1);

    int fd;
    char* buffer;
    char* readbuffer;

    if (!FLAGS_verify_io) {
        goto skip_write_io;
    }

    fd = Open(filename.c_str());

    if (fd == -1) {
        LOG(FATAL) << "open file failed!";
        return -1;
    }

    buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    uint64_t offset_base;
    for (int i = 0; i < 16; i ++) {
        // unsigned int offset = rand_r(&offset_base) %
        //                       (FLAGS_test_disk_size - 4096);
        // offset -= offset % FLAGS_chunk_size;
        uint64_t offset = i * FLAGS_chunk_size;
        // *(unsigned int*)buffer = offset;
        Write(fd, buffer, offset, 4096);
    }

    // Test I/O at address >= 4G
    // offset_base = 4ul * 64 * FLAGS_chunk_size;
    // Write(fd, buffer, offset_base, 4096);
    // Write(fd, buffer, offset_base, 4096);
    // Write(fd, buffer, offset_base, 4096);

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.op = LIBCURVE_OP_WRITE;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = callback;

    AioWrite(fd, &writeaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    AioWrite(fd, &writeaioctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    writeaioctx.op = LIBCURVE_OP_READ;
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

    LOG(INFO) << "LibCurve I/O verified for stage 1, going to read repeatedly";

skip_write_io:
    std::atomic<bool> stop(false);
    auto testfunc = [&]() {
        while (!stop.load()) {
            if (!FLAGS_verify_io) {
                goto skip_read_io;
            }

            AioRead(fd, &readaioctx);
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

skip_read_io:
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    };

    std::thread t(testfunc);
    char c;

    if (FLAGS_io_time > 0) {
        sleep(FLAGS_io_time);
    } else {
        std::cin >> c;
    }

    stop.store(true);
    if (t.joinable()) {
        t.join();
    }

    if (FLAGS_fake_mds) {
        mds.UnInitialize();
    }

    if (!FLAGS_verify_io) {
        goto workflow_finish;
    }
    delete buffer;
    delete readbuffer;
    Close(fd);

workflow_finish:
    return 0;
}
