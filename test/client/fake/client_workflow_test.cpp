/*
 * Project: curve
 * File Created: Saturday, 13th October 2018 1:59:08 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fcntl.h>  // NOLINT
#include <string>
#include <iostream>
#include <atomic>
#include <thread>   // NOLINT
#include <chrono>   // NOLINT

#include "include/client/libcurve_qemu.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string metaserver_addr = "127.0.0.1:6666";   // NOLINT

DECLARE_uint64(test_disk_size);
DEFINE_uint32(io_time, 5, "Duration for I/O test");
DEFINE_bool(fake_mds, true, "create fake mds");
DEFINE_bool(create_copysets, false, "create copysets on chunkserver");
DEFINE_bool(verify_io, true, "verify read/write I/O getting done correctly");

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
    LOG(INFO) << "aio call back here, errorcode = " << context->err;
}
void readcallbacktest(CurveAioContext* context) {
    readflag = true;
    interfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->err;
}

int main(int argc, char ** argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);
    std::string configpath = "./client.conf";   // NOLINT
    std::string config = ""\
    "metaserver_addr=127.0.0.1:6666\n" \
    "getLeaderRetry=3\n"\
    "queueCapacity=4096\n"\
    "threadpoolSize=2\n"\
    "opRetryIntervalUs=200000\n"\
    "opMaxRetry=3\n"\
    "pre_allocate_context_num=1024\n"\
    "ioSplitMaxSize=64\n"\
    "enableAppliedIndexRead=1\n"\
    "loglevel=0";

    int fd_ =  open(configpath.c_str(), O_CREAT | O_RDWR);
    int len = write(fd_, config.c_str(), config.length());
    close(fd_);

    if (Init(configpath.c_str()) != 0) {
        LOG(FATAL) << "Fail to init config";
    }

    std::string filename = "1_userinfo_test.txt";
    // uint64_t size = FLAGS_test_disk_size;

    /*** init mds service ***/
    FakeMDS mds(filename);
    if (FLAGS_fake_mds) {
        mds.Initialize();
        mds.StartService();
        if (FLAGS_create_copysets) {
            mds.CreateCopysetNode();
        }
    }

    /**** libcurve file operation ****/
    int filedesc = Open(filename.c_str(), FLAGS_test_disk_size, true);
    Close(filedesc);

    sleep(1);

    int fd;
    char* buffer;
    char* readbuffer;
    if (!FLAGS_verify_io) {
        goto skip_write_io;
    }

    fd = Open(filename.c_str(), 0, false);

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
        uint64_t offset = i * chunk_size;
        Write(fd, buffer, offset, 4096);
    }

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.op = LIBCURVE_OP_WRITE;
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

    readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    writeaioctx.op = LIBCURVE_OP_READ;
    readaioctx.cb = readcallbacktest;
    AioRead(fd, &readaioctx);
    {
        std::unique_lock<std::mutex> lk(interfacemtx);
        interfacecv.wait(lk, []()->bool{return readflag;});
    }

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
            readflag = false;
            AioRead(fd, &readaioctx);
            {
                std::unique_lock<std::mutex> lk(interfacemtx);
                interfacecv.wait(lk, []()->bool{return readflag;});
            }
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

    Close(fd);
    UnInit();

    if (FLAGS_fake_mds) {
        mds.UnInitialize();
    }

    if (!FLAGS_verify_io) {
        goto workflow_finish;
    }
    delete[] buffer;
    delete[] readbuffer;

workflow_finish:
    unlink(configpath.c_str());
    return 0;
}
