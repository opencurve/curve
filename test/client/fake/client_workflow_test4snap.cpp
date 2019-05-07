/*
 * Project: curve
 * File Created: Monday, 7th January 2019 10:04:50 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fcntl.h>  // NOLINT
#include <string>
#include <iostream>
#include <atomic>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT

#include "src/client/client_common.h"
#include "src/client/libcurve_define.h"
#include "include/client/libcurve.h"
#include "src/client/libcurve_snapshot.h"
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

using curve::client::UserInfo_t;
using curve::client::ChunkServerAddr;
using curve::client::EndPoint;
using curve::client::SegmentInfo;
using curve::client::ChunkInfoDetail;
using curve::client::SnapshotClient;
using curve::client::ChunkID;
using curve::client::LogicPoolID;
using curve::client::CopysetID;
using curve::client::ChunkIDInfo;
using curve::client::CopysetInfo_t;
using curve::client::MetaCache;
using curve::client::LogicalPoolCopysetIDInfo;

int main(int argc, char ** argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);

    std::string filename = "/1_userinfo_test.txt";
    /*** init mds service ***/
    FakeMDS mds(filename);
    if (FLAGS_fake_mds) {
        mds.Initialize();
        mds.StartService();
        if (FLAGS_create_copysets) {
            // 设置leaderid
            EndPoint ep;
            butil::str2endpoint("127.0.0.1", 8200, &ep);
            PeerId pd(ep);
            mds.StartCliService(pd);
            mds.CreateCopysetNode(true);
        }
    }

    ClientConfigOption_t opt;
    opt.metaServerOpt.rpcTimeoutMs = 500;
    opt.metaServerOpt.rpcRetryTimes = 3;
    opt.metaServerOpt.metaaddrvec.push_back("127.0.0.1:6666");
    opt.ioOpt.reqSchdulerOpt.queueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.threadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    opt.ioOpt.metaCacheOpt.getLeaderRetry = 3;
    opt.ioOpt.ioSenderOpt.enableAppliedIndexRead = 1;
    opt.ioOpt.ioSplitOpt.ioSplitMaxSize = 64;
    opt.loginfo.loglevel = 0;

    SnapshotClient cl;
    if (cl.Init(opt) != 0) {
        LOG(FATAL) << "Fail to init config";
        return -1;
    }

    UserInfo_t userinfo;
    userinfo.owner = "test";

    uint64_t seq = 0;
    if (-1 == cl.CreateSnapShot(filename, userinfo, &seq)) {
        LOG(ERROR) << "create failed!";
        return -1;
    }

    SegmentInfo seginfo;
    LogicalPoolCopysetIDInfo lpcsIDInfo;
    if (LIBCURVE_ERROR::FAILED == cl.GetSnapshotSegmentInfo(filename,
                                                        userinfo,
                                                        &lpcsIDInfo,
                                                        0, 0,
                                                        &seginfo)) {
        LOG(ERROR) << "GetSnapshotSegmentInfo failed!";
        return -1;
    }

    curve::client::FInfo_t sinfo;
    if (-1 == cl.GetSnapShot(filename, userinfo, seq, &sinfo)) {
        LOG(ERROR) << "ListSnapShot failed!";
        return -1;
    }

    char* readbuf = new char[8192];
    cl.ReadChunkSnapshot(ChunkIDInfo(1, 10000, 1), 1, 0, 8192, static_cast<void*>(readbuf));    // NOLINT
    for (int i = 0; i < 8192; i++) {
        if (readbuf[i] != 1) {
            LOG(ERROR) << "read snap chunk failed!";
        }
    }

    cl.DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo(1, 10000, 1), 2);

    ChunkInfoDetail *chunkInfo = new ChunkInfoDetail;
    cl.GetChunkInfo(ChunkIDInfo(1, 10000, 1), chunkInfo);
    for (auto iter : chunkInfo->chunkSn) {
        if (iter != 1111) {
            LOG(ERROR) << "chunksn read failed!";
        }
    }
    cl.UnInit();
    return 0;
}
