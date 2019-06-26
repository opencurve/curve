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

#include "include/client/libcurve.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/client_common.h"

using curve::client::ChunkServerAddr;
using curve::client::EndPoint;

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string metaserver_addr = "127.0.0.1:9104";   // NOLINT

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
}
void readcallbacktest(CurveAioContext* context) {
    readflag = true;
    interfacecv.notify_one();
}

int main(int argc, char ** argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);
    std::string configpath = "./test/client/testConfig/client.conf";

    if (Init(configpath.c_str()) != 0) {
        LOG(FATAL) << "Fail to init config";
    }

    // filename必须是全路径
    std::string filename = "/1_userinfo_";
    // uint64_t size = FLAGS_test_disk_size;

    /*** init mds service ***/
    FakeMDS mds(filename);
    if (FLAGS_fake_mds) {
        mds.Initialize();
        mds.StartService();
        if (FLAGS_create_copysets) {
            // 设置leaderid
            EndPoint ep;
            butil::str2endpoint("127.0.0.1", 9106, &ep);
            PeerId pd(ep);
            mds.StartCliService(pd);
            mds.CreateCopysetNode(true);
        }
    }

    /**** libcurve file operation ****/
    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    Create(filename.c_str(), &userinfo, FLAGS_test_disk_size);

    sleep(1);

    int fd;
    char* buffer;
    char* readbuffer;
    if (!FLAGS_verify_io) {
        // goto skip_write_io;
    }

    fd = Open(filename.c_str(), &userinfo);

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



    char* buf2 = new char[8 * 1024];
    CurveAioContext* aioctx1 = new CurveAioContext;
    char* buf1 = new char[8 * 1024];
    CurveAioContext* aioctx2 = new CurveAioContext;
    auto f = [&]() {
        while (1) {
            for (int i = 0; i < 10; i++) {
                aioctx1->buf = buf1;
                aioctx1->offset = 0;
                aioctx1->op = LIBCURVE_OP_WRITE;
                aioctx1->length = 8 * 1024;
                aioctx1->cb = writecallbacktest;
                AioWrite(fd, aioctx1);
                aioctx2->buf = buf2;
                aioctx2->offset = 0;
                aioctx2->length = 8 * 1024;
                aioctx2->op = LIBCURVE_OP_READ;
                aioctx2->cb = readcallbacktest;
                AioRead(fd, aioctx2);
            }

            char buf1[4096];
            char buf2[8192];
            Write(fd, buf2, 0, 8192);
            Read(fd, buf1, 0, 4096);
        }
    };

    std::thread t1(f);
    std::thread t2(f);

    t1.join();
    t2.join();

    delete[] buf1;
    delete[] buf2;
    delete aioctx2;
    delete aioctx1;

    CurveAioContext writeaioctx;
    CurveAioContext readaioctx;
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
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
