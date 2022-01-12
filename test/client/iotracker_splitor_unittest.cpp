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
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:17:04 pm
 * Author: tongguangxun
 */

#include <brpc/server.h>
#include <fiu-control.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>              //NOLINT
#include <condition_variable>  //NOLINT
#include <mutex>               // NOLINT
#include <string>
#include <thread>              //NOLINT

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/config_info.h"
#include "src/client/file_instance.h"
#include "src/client/io_tracker.h"
#include "src/client/iomanager4file.h"
#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"
#include "src/client/mds_client.h"
#include "src/client/metacache.h"
#include "src/client/metacache_struct.h"
#include "src/client/request_context.h"
#include "src/client/splitor.h"
#include "src/client/source_reader.h"
#include "test/client/fake/fakeMDS.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/mock_schedule.h"

extern std::string mdsMetaServerAddr;
extern uint32_t chunk_size;
extern std::string configpath;

extern butil::IOBuf writeData;

namespace curve {
namespace client {

bool ioreadflag = false;
std::mutex readmtx;
std::condition_variable readcv;
bool iowriteflag = false;
std::mutex writemtx;
std::condition_variable writecv;

void readcallback(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
    ioreadflag = true;
    readcv.notify_one();
}

void writecallback(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
    iowriteflag = true;
    writecv.notify_one();
}

class IOTrackerSplitorTest : public ::testing::Test {
 public:
    void SetUp() {
        fiu_init(0);
        fopt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");
        fopt.metaServerOpt.rpcRetryOpt.rpcTimeoutMs = 500;
        fopt.metaServerOpt.rpcRetryOpt.rpcRetryIntervalUS = 50000;
        fopt.loginfo.logLevel = 0;
        fopt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
        fopt.ioOpt.ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;
        fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 1000;
        fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
        fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;   // NOLINT
        fopt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
        fopt.ioOpt.metaCacheOpt.metacacheRPCRetryIntervalUS = 500;
        fopt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
        fopt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
        fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
        fopt.ioOpt.closeFdThreadOption.fdTimeout = 3;
        fopt.ioOpt.closeFdThreadOption.fdCloseTimeInterval = 5;
        fopt.leaseOpt.mdsRefreshTimesPerLease = 4;

        fileinstance_ = new FileInstance();
        userinfo.owner = "userinfo";
        userinfo.password = "12345";

        mdsclient_ = std::make_shared<MDSClient>();
        mdsclient_->Initialize(fopt.metaServerOpt);
        fileinstance_->Initialize("/test", mdsclient_, userinfo, OpenFlags{},
                                  fopt);
        InsertMetaCache();

        SourceReader::GetInstance().SetOption(fopt);
        SourceReader::GetInstance().SetReadHandlers({});
        SourceReader::GetInstance().Run();
    }

    void TearDown() {
        writeData.clear();
        fileinstance_->Close();
        fileinstance_->UnInitialize();
        mdsclient_.reset();
        delete fileinstance_;

        SourceReader::GetInstance().Stop();

        LOG(INFO) << "DONE!";
        server.Stop(0);
        server.Join();
    }

    void InsertMetaCache() {
        if (server.AddService(&curvefsservice,
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }
        if (server.AddService(&topologyservice,
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }
        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (server.Start(mdsMetaServerAddr.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to start Server";
        }

        /**
         * 1. set openfile response
         */
        ::curve::mds::OpenFileResponse* openresponse =
        new ::curve::mds::OpenFileResponse();
        ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
        se->set_sessionid("1");
        se->set_createtime(12345);
        se->set_leasetime(10000000);
        se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

        ::curve::mds::FileInfo* fin = new ::curve::mds::FileInfo;
        fin->set_filename("1_userinfo_.txt");
        fin->set_owner("userinfo");
        fin->set_id(1);
        fin->set_parentid(0);
        fin->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        fin->set_chunksize(4 * 1024 * 1024);
        fin->set_length(1 * 1024 * 1024 * 1024ul);
        fin->set_ctime(12345678);
        fin->set_seqnum(0);
        fin->set_segmentsize(1 * 1024 * 1024 * 1024ul);

        openresponse->set_statuscode(::curve::mds::StatusCode::kOK);
        openresponse->set_allocated_protosession(se);
        openresponse->set_allocated_fileinfo(fin);
        FakeReturn* openfakeret = new FakeReturn(nullptr, static_cast<void*>(openresponse));      // NOLINT
        curvefsservice.SetOpenFile(openfakeret);
        // open will set the finfo for file instance
        fileinstance_->Open("1_userinfo_.txt", userinfo);

        /**
         * 2. set closefile response
         */
        ::curve::mds::CloseFileResponse* closeresp = new ::curve::mds::CloseFileResponse;    // NOLINT
        closeresp->set_statuscode(::curve::mds::StatusCode::kOK);
        FakeReturn* closefileret
        = new FakeReturn(nullptr, static_cast<void*>(closeresp));
        curvefsservice.SetCloseFile(closefileret);

        /**
         * 3. 设置GetOrAllocateSegmentresponse
         */
        curve::mds::GetOrAllocateSegmentResponse* response =
            new curve::mds::GetOrAllocateSegmentResponse();
        curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;

        response->set_statuscode(::curve::mds::StatusCode::kOK);
        response->set_allocated_pagefilesegment(pfs);
        response->mutable_pagefilesegment()->
            set_logicalpoolid(1234);
        response->mutable_pagefilesegment()->
            set_segmentsize(1 * 1024 * 1024 * 1024);
        response->mutable_pagefilesegment()->
            set_chunksize(4 * 1024 * 1024);
        response->mutable_pagefilesegment()->
            set_startoffset(0);

        for (int i = 0; i < 256; i ++) {
            auto chunk = response->mutable_pagefilesegment()->add_chunks();
            chunk->set_copysetid(i);
            chunk->set_chunkid(i);
        }
        getsegmentfakeret = new FakeReturn(nullptr,
                    static_cast<void*>(response));
        curvefsservice.SetGetOrAllocateSegmentFakeReturn(getsegmentfakeret);

        curve::mds::GetOrAllocateSegmentResponse* notallocateresponse =
                            new curve::mds::GetOrAllocateSegmentResponse();
        notallocateresponse->set_statuscode(::curve::mds::StatusCode
                                            ::kSegmentNotAllocated);
        notallocatefakeret = new FakeReturn(nullptr,
                                    static_cast<void*>(notallocateresponse));

        // set GetOrAllocateSegmentResponse for read from clone source
        curve::mds::GetOrAllocateSegmentResponse* cloneSourceResponse =
            new curve::mds::GetOrAllocateSegmentResponse();
        curve::mds::PageFileSegment* clonepfs = new curve::mds::PageFileSegment;

        cloneSourceResponse->set_statuscode(::curve::mds::StatusCode::kOK);
        cloneSourceResponse->set_allocated_pagefilesegment(clonepfs);
        cloneSourceResponse->mutable_pagefilesegment()->
            set_logicalpoolid(1);
        cloneSourceResponse->mutable_pagefilesegment()->
            set_segmentsize(1 * 1024 * 1024 * 1024);
        cloneSourceResponse->mutable_pagefilesegment()->
            set_chunksize(4 * 1024 * 1024);
        cloneSourceResponse->mutable_pagefilesegment()->
            set_startoffset(1 * 1024 * 1024 * 1024);

        for (int i = 256; i < 512; i++) {
            auto chunk = cloneSourceResponse->mutable_pagefilesegment()
                                                        ->add_chunks();
            chunk->set_copysetid(i);
            chunk->set_chunkid(i);
        }
        getsegmentfakeretclone = new FakeReturn(nullptr,
                    static_cast<void*>(cloneSourceResponse));

        /**
         * 4. set refresh response
         */
        curve::mds::FileInfo * info = new curve::mds::FileInfo;
        info->set_filename("1_userinfo_.txt");
        info->set_seqnum(2);
        info->set_id(1);
        info->set_parentid(0);
        info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        info->set_chunksize(4 * 1024 * 1024);
        info->set_length(1 * 1024 * 1024 * 1024ul);
        info->set_ctime(12345678);

        ::curve::mds::ReFreshSessionResponse* refreshresp =
            new ::curve::mds::ReFreshSessionResponse;
        refreshresp->set_statuscode(::curve::mds::StatusCode::kOK);
        refreshresp->set_sessionid("1234");
        refreshresp->set_allocated_fileinfo(info);
        FakeReturn* refreshfakeret
        = new FakeReturn(nullptr, static_cast<void*>(refreshresp));
        curvefsservice.SetRefreshSession(refreshfakeret, nullptr);

        /**
         * 5. 设置topology返回值
         */
        ::curve::mds::topology::GetChunkServerListInCopySetsResponse* response_1
        = new ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
        response_1->set_statuscode(0);
        uint64_t chunkserveridc = 1;
        for (int i = 0; i < 256; i ++) {
            auto csinfo = response_1->add_csinfo();
            csinfo->set_copysetid(i);

            for (int j = 0; j < 3; j++) {
                auto cslocs = csinfo->add_cslocs();
                cslocs->set_chunkserverid(chunkserveridc++);
                cslocs->set_hostip("127.0.0.1");
                cslocs->set_port(9104);
            }
        }
        FakeReturn* faktopologyeret = new FakeReturn(nullptr,
                    static_cast<void*>(response_1));
        topologyservice.SetFakeReturn(faktopologyeret);

        curve::client::MetaCache* mc = fileinstance_->GetIOManager4File()->
                                                            GetMetaCache();
        curve::client::FInfo_t fi;
        fi.userinfo = userinfo;
        fi.chunksize   = 4 * 1024 * 1024;
        fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
        SegmentInfo sinfo;
        LogicalPoolCopysetIDInfo_t lpcsIDInfo;
        mdsclient_->GetOrAllocateSegment(true, 0, &fi, &sinfo);
        int count = 0;
        for (auto iter : sinfo.chunkvec) {
            uint64_t index = (sinfo.startoffset + count*fi.chunksize )
                            / fi.chunksize;
            mc->UpdateChunkInfoByIndex(index, iter);
            ++count;
        }

        std::vector<CopysetInfo<ChunkServerID>> cpinfoVec;
        mdsclient_->GetServerList(lpcsIDInfo.lpid, lpcsIDInfo.cpidVec,
                                  &cpinfoVec);

        for (auto iter : cpinfoVec) {
            mc->UpdateCopysetInfo(lpcsIDInfo.lpid, iter.cpid_, iter);
        }

        // 5. set close response
        auto* closeResp = new ::curve::mds::CloseFileResponse();
        closeResp->set_statuscode(::curve::mds::StatusCode::kOK);
        auto* closeFakeRet =
            new FakeReturn(nullptr, static_cast<void*>(closeResp));
        curvefsservice.SetCloseFile(closeFakeRet);
    }

    FileClient *fileClient_;
    UserInfo_t userinfo;
    std::shared_ptr<MDSClient> mdsclient_;
    FileServiceOption fopt;
    FileInstance *fileinstance_;
    brpc::Server server;
    FakeMDSCurveFSService curvefsservice;
    FakeTopologyService topologyservice;
    FakeReturn *getsegmentfakeret;
    FakeReturn *notallocatefakeret;
    FakeReturn *getsegmentfakeretclone;
};

TEST_F(IOTrackerSplitorTest, AsyncStartRead) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    iomana->SetRequestScheduler(mockschuler);

    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;  // 4M - 4k
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;  // 4M + 8K
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = readcallback;
    aioctx.buf = new char[aioctx.length];
    aioctx.op = LIBCURVE_OP::LIBCURVE_OP_READ;

    ioreadflag = false;
    char* data = static_cast<char*>(aioctx.buf);
    iomana->AioRead(&aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(readmtx);
        readcv.wait(lk, []()->bool{return ioreadflag;});
    }
    LOG(ERROR) << "address = " << &data;
    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('e', data[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('f', data[4 * 1024 + chunk_size]);
    ASSERT_EQ('f', data[aioctx.length - 1]);

    delete[] data;
}

TEST_F(IOTrackerSplitorTest, AsyncStartWrite) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    iomana->SetRequestScheduler(mockschuler);

    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = writecallback;
    aioctx.buf = new char[aioctx.length];
    aioctx.op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    char* data = static_cast<char*>(aioctx.buf);

    memset(data, 'a', 4 * 1024);
    memset(data + 4 * 1024, 'b', chunk_size);
    memset(data + 4 * 1024 + chunk_size, 'c', 4 * 1024);
    FInfo_t fi;
    fi.chunksize = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
    iowriteflag = false;
    iomana->AioWrite(&aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(writemtx);
        writecv.wait(lk, []() -> bool { return iowriteflag; });
    }

    std::unique_ptr<char[]> writebuffer(new char[aioctx.length]);
    memcpy(writebuffer.get(), writeData.to_string().c_str(), aioctx.length);

    // check butil::IOBuf write data
    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + chunk_size]);
    ASSERT_EQ('c', writebuffer[aioctx.length - 1]);

    delete[] data;
}

TEST_F(IOTrackerSplitorTest, StartRead) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    iomana->SetRequestScheduler(mockschuler);

    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    auto threadfunc = [&]() {
        iomana->Read(data, offset, length, mdsclient_.get());
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('e', data[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('f', data[4 * 1024 + chunk_size]);
    ASSERT_EQ('f', data[length - 1]);

    delete[] data;
}

TEST_F(IOTrackerSplitorTest, StartWrite) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    iomana->SetRequestScheduler(mockschuler);

    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    memset(buf, 'a', 4 * 1024);
    memset(buf + 4 * 1024, 'b', chunk_size);
    memset(buf + 4 * 1024 + chunk_size, 'c', 4 * 1024);

    auto threadfunc = [&]() {
        iomana->Write(buf, offset, length, mdsclient_.get());
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    std::unique_ptr<char[]> writebuffer(new char[length]);
    memcpy(writebuffer.get(), writeData.to_string().c_str(), length);

    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + chunk_size]);
    ASSERT_EQ('c', writebuffer[length - 1]);

    delete[] buf;
}

TEST_F(IOTrackerSplitorTest, ManagerAsyncStartRead) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);
    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx->length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = readcallback;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_READ;

    ioreadflag = false;
    char* data = static_cast<char*>(aioctx->buf);
    ioctxmana->AioRead(aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(readmtx);
        readcv.wait(lk, []()->bool{return ioreadflag;});
    }
    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('e', data[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('f', data[4 * 1024 + chunk_size]);
    ASSERT_EQ('f', data[aioctx->length - 1]);
}

TEST_F(IOTrackerSplitorTest, ManagerAsyncStartWrite) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);

    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx->length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = writecallback;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    char* data = static_cast<char*>(aioctx->buf);

    memset(data, 'a', 4 * 1024);
    memset(data + 4 * 1024, 'b', chunk_size);
    memset(data + 4 * 1024 + chunk_size, 'c', 4 * 1024);

    iowriteflag = false;
    ioctxmana->AioWrite(aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(writemtx);
        writecv.wait(lk, []()->bool{return iowriteflag;});
    }

    std::unique_ptr<char[]> writebuffer(new char[aioctx->length]);
    memcpy(writebuffer.get(), writeData.to_string().c_str(), aioctx->length);

    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + chunk_size]);
    ASSERT_EQ('c', writebuffer[aioctx->length - 1]);
}

/*
TEST_F(IOTrackerSplitorTest, ManagerAsyncStartWriteReadGetSegmentFail) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::mds::GetOrAllocateSegmentResponse* response =
                new curve::mds::GetOrAllocateSegmentResponse();
    brpc::Controller* controller1 = new brpc::Controller;
    controller1->SetFailed(-1, "error");
    FakeReturn* fakeret = new FakeReturn(controller1,
                static_cast<void*>(response));
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(fakeret);

    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);
    ioctxmana->SetIOOpt(fopt.ioOpt);

    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 10*1024*1024*1024ul;
    aioctx->length = chunk_size + 8 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = writecallback;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    char* data = static_cast<char*>(aioctx->buf);

    memset(data, 'a', 4 * 1024);
    memset(data + 4 * 1024, 'b', chunk_size);
    memset(data + 4 * 1024 + chunk_size, 'c', 4 * 1024);

    // 设置mds一侧get segment接口返回失败，底层task thread层会一直重试，
    // 但是不会阻塞上层继续向下发送IO请求
    int reqcount = 32;
    auto threadFunc1 = [&]() {
        while (reqcount > 0) {
            fileinstance_->AioWrite(aioctx);
            reqcount--;
        }
    };

    std::thread t1(threadFunc1);
    std::thread t2(threadFunc1);
    t1.join();
    t2.join();
}

TEST_F(IOTrackerSplitorTest, ManagerAsyncStartWriteReadGetServerlistFail) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse* response
        = new ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
    brpc::Controller* controller1 = new brpc::Controller;
    controller1->SetFailed(-1, "error");
    FakeReturn* fakeret = new FakeReturn(controller1,
                static_cast<void*>(response));
    topologyservice.SetFakeReturn(fakeret);

    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);
    ioctxmana->SetIOOpt(fopt.ioOpt);

    // offset 10*1024*1024*1024ul 不在metacache里
    // client回去mds拿segment和serverlist
    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 10*1024*1024*1024ul;
    aioctx->length = chunk_size + 8 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = writecallback;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    char* data = static_cast<char*>(aioctx->buf);

    memset(data, 'a', 4 * 1024);
    memset(data + 4 * 1024, 'b', chunk_size);
    memset(data + 4 * 1024 + chunk_size, 'c', 4 * 1024);

    // 设置mds一侧get server list接口返回失败，底层task thread层会一直重试
    // 但是不会阻塞，上层继续向下发送IO请求
    int reqcount = 32;
    auto threadFunc1 = [&]() {
        while (reqcount > 0) {
            fileinstance_->AioWrite(aioctx);
            reqcount--;
        }
    };

    std::thread t1(threadFunc1);
    std::thread t2(threadFunc1);
    t1.join();
    t2.join();
}
*/
TEST_F(IOTrackerSplitorTest, ManagerStartRead) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);

    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    auto threadfunc = [&]() {
        ASSERT_EQ(length,
                  ioctxmana->Read(data, offset, length, mdsclient_.get()));
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('e', data[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('f', data[4 * 1024 + chunk_size]);
    ASSERT_EQ('f', data[length - 1]);
    delete[] data;
}

TEST_F(IOTrackerSplitorTest, ManagerStartWrite) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);

    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    memset(buf, 'a', 4 * 1024);
    memset(buf + 4 * 1024, 'b', chunk_size);
    memset(buf + 4 * 1024 + chunk_size, 'c', 4 * 1024);

    auto threadfunc = [&]() {
        ioctxmana->Write(buf, offset, length, mdsclient_.get());
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

     std::unique_ptr<char[]> writebuffer(new char[length]);
    memcpy(writebuffer.get(), writeData.to_string().c_str(), length);

    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + chunk_size]);
    ASSERT_EQ('c', writebuffer[length - 1]);

    delete[] buf;
}

TEST_F(IOTrackerSplitorTest, ExceptionTest_TEST) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();
    FInfo_t fi;
    fi.chunksize = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
    auto fileserv = new FileInstance();

    UserInfo_t rootuserinfo;
    rootuserinfo.owner = "root";
    rootuserinfo.password = "root_password";

    ASSERT_TRUE(fileserv->Initialize("/test", mdsclient_, rootuserinfo,
                                     OpenFlags{}, fopt));
    ASSERT_EQ(LIBCURVE_ERROR::OK, fileserv->Open("1_userinfo_.txt", userinfo));
    curve::client::IOManager4File* iomana = fileserv->GetIOManager4File();
    MetaCache* mc = fileserv->GetIOManager4File()->GetMetaCache();

    FileMetric fileMetric("/test");
    IOTracker* iotracker = new IOTracker(iomana, mc, mockschuler, &fileMetric);

    ASSERT_NE(nullptr, iotracker);
    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    auto waitfunc = [&]() {
        int retlen = iotracker->Wait();
        ASSERT_EQ(-LIBCURVE_ERROR::FAILED, retlen);
    };

    auto threadfunc = [&]() {
        iotracker->SetUserDataType(UserDataType::RawBuffer);
        iotracker->StartWrite(nullptr, offset, length, mdsclient_.get(), &fi);
    };

    std::thread process(threadfunc);
    std::thread waitthread(waitfunc);

    uint64_t off = 4 * 1024 * 1024 * 1024ul - 4 * 1024;
    uint64_t len = 4 * 1024 * 1024 + 8 * 1024;
    iomana->Write(buf, off, len, mdsclient_.get());

    if (process.joinable()) {
        process.join();
    }
    if (waitthread.joinable()) {
        waitthread.join();
    }
    fileserv->Close();
    fileserv->UnInitialize();
    delete fileserv;
    delete mockschuler;
}

TEST_F(IOTrackerSplitorTest, BoundaryTEST) {
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    auto ioctxmana = fileinstance_->GetIOManager4File();
    ioctxmana->SetRequestScheduler(mockschuler);

    // this offset and length will make splitor split fail.
    // we set disk size = 1G.
    uint64_t offset = 1 * 1024 * 1024 * 1024 -
                     4 * 1024 * 1024 - 4 *1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    memset(buf, 'a', 4 * 1024);
    memset(buf + 4 * 1024, 'b', chunk_size);
    memset(buf + 4 * 1024 + chunk_size, 'c', 4 * 1024);

    auto threadfunc = [&]() {
        ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED,
                  ioctxmana->Write(buf, offset, length, mdsclient_.get()));
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    delete[] buf;
}

TEST_F(IOTrackerSplitorTest, largeIOTest) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();
    /**
     * this offset and length will make splitor split into two 8k IO.
     */
    uint64_t length = 2 * 64 * 1024;  // 128KB
    uint64_t offset = 4 * 1024 * 1024 - length;  // 4MB - 128KB
    char* buf = new char[length];


    memset(buf, 'a', 64 * 1024);              // 64KB
    memset(buf + 64 * 1024, 'b', 64 * 1024);  // 64KB
    butil::IOBuf writeData;
    writeData.append(buf, length);
    FInfo_t fi;
    fi.seqnum = 0;
    fi.chunksize = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();

    IOTracker* iotracker = new IOTracker(iomana, mc, &mockschuler);
    iotracker->SetOpType(OpType::WRITE);

    curve::client::ChunkIDInfo chinfo(1, 2, 3);
    mc->UpdateChunkInfoByIndex(0, chinfo);

    std::vector<RequestContext*> reqlist;
    auto dataCopy = writeData;
    ASSERT_EQ(0, curve::client::Splitor::IO2ChunkRequests(
                     iotracker, mc, &reqlist, &dataCopy, offset, length,
                     mdsclient_.get(), &fi));
    ASSERT_EQ(2, reqlist.size());

    RequestContext* first = reqlist.front();
    reqlist.erase(reqlist.begin());
    RequestContext* second = reqlist.front();
    reqlist.erase(reqlist.begin());

    // first 64KB is 'a'
    // seconds 64KB is 'b'
    butil::IOBuf splitData;
    splitData.append(first->writeData_);
    splitData.append(second->writeData_);
    ASSERT_EQ(writeData, splitData);

    ASSERT_EQ(1, first->idinfo_.cid_);
    ASSERT_EQ(3, first->idinfo_.cpid_);
    ASSERT_EQ(2, first->idinfo_.lpid_);
    ASSERT_EQ(4 * 1024 * 1024 - length, first->offset_);
    ASSERT_EQ(64 * 1024, first->rawlength_);
    ASSERT_EQ(0, first->seq_);
    ASSERT_EQ(0, first->appliedindex_);

    ASSERT_EQ(1, second->idinfo_.cid_);
    ASSERT_EQ(3, second->idinfo_.cpid_);
    ASSERT_EQ(2, second->idinfo_.lpid_);
    ASSERT_EQ(4 * 1024 * 1024 - 64 * 1024, second->offset_);
    ASSERT_EQ(64 * 1024, second->rawlength_);
    ASSERT_EQ(0, second->seq_);
    ASSERT_EQ(0, second->appliedindex_);
    delete[] buf;
}

TEST_F(IOTrackerSplitorTest, InvalidParam) {
    uint64_t length = 2 * 64 * 1024;
    uint64_t offset = 4 * 1024 * 1024 - length;
    char* buf = new char[length];
    butil::IOBuf iobuf;
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    std::vector<RequestContext*> reqlist;
    FInfo_t fi;

    IOTracker* iotracker = new IOTracker(nullptr, nullptr, nullptr);
    curve::client::ChunkIDInfo cid(0, 0, 0);

    ASSERT_EQ(-1, curve::client::Splitor::IO2ChunkRequests(
                      nullptr, mc, &reqlist, &iobuf, offset, length,
                      mdsclient_.get(), &fi));

    ASSERT_EQ(-1, curve::client::Splitor::SingleChunkIO2ChunkRequests(
                      nullptr, mc,
                      &reqlist, cid, &iobuf, offset, length, 0));

    ASSERT_EQ(-1, curve::client::Splitor::IO2ChunkRequests(
                      iotracker, nullptr, &reqlist, &iobuf, offset, length,
                      mdsclient_.get(), nullptr));

    ASSERT_EQ(-1, curve::client::Splitor::SingleChunkIO2ChunkRequests(
                      iotracker, nullptr,
                      &reqlist, cid, &iobuf, offset, length, 0));

    ASSERT_EQ(-1, curve::client::Splitor::IO2ChunkRequests(
                      iotracker, mc, &reqlist, &iobuf, offset, length,
                      mdsclient_.get(), nullptr));

    ASSERT_EQ(
        -1, curve::client::Splitor::IO2ChunkRequests(
                iotracker, mc, &reqlist, &iobuf, offset, length, nullptr, &fi));

    ASSERT_EQ(0, curve::client::Splitor::SingleChunkIO2ChunkRequests(
                     iotracker, mc,
                     &reqlist, cid, &iobuf, offset, length, 0));

    ASSERT_EQ(-1, curve::client::Splitor::IO2ChunkRequests(
                      iotracker, mc, nullptr, &iobuf, offset, length,
                      mdsclient_.get(), nullptr));

    ASSERT_EQ(-1, curve::client::Splitor::SingleChunkIO2ChunkRequests(
                      iotracker, mc,
                      nullptr, cid, &iobuf, offset, length, 0));

    ASSERT_EQ(-1, curve::client::Splitor::IO2ChunkRequests(
                      iotracker, mc, &reqlist, nullptr, offset, length,
                      mdsclient_.get(), nullptr));

    iotracker->SetOpType(OpType::WRITE);
    ASSERT_EQ(-1,
              curve::client::Splitor::SingleChunkIO2ChunkRequests(
                  iotracker, mc, &reqlist, cid, nullptr, offset, length, 0));

    // write request, but write data is nullptr
    iotracker->SetOpType(OpType::WRITE);
    ASSERT_EQ(-1, curve::client::Splitor::IO2ChunkRequests(
                      iotracker, mc, &reqlist, nullptr, offset, length,
                      mdsclient_.get(), &fi));

    // ASSERT_EQ(-1, Splitor::CalcDiscardSegments(nullptr));

    delete iotracker;
    delete[] buf;
}

TEST_F(IOTrackerSplitorTest, RequestSourceInfoTest) {
    IOTracker ioTracker(nullptr, nullptr, nullptr);
    ioTracker.SetOpType(OpType::READ);

    MetaCache metaCache;
    FInfo_t fileInfo;
    fileInfo.chunksize = 16 * 1024 * 1024;          // 16M
    fileInfo.filestatus = FileStatus::CloneMetaInstalled;

    CloneSourceInfo cloneSourceInfo;
    cloneSourceInfo.name = "/clonesource";
    cloneSourceInfo.length = 10ull * 1024 * 1024 * 1024;      // 10GB
    cloneSourceInfo.segmentSize = 1ull * 1024 * 1024 * 1024;  // 1GB

    // 源卷只分配了第一个和最后一个segment
    cloneSourceInfo.allocatedSegmentOffsets.insert(0);
    cloneSourceInfo.allocatedSegmentOffsets.insert(cloneSourceInfo.length -
                                                   cloneSourceInfo.segmentSize);

    fileInfo.sourceInfo = cloneSourceInfo;
    metaCache.UpdateFileInfo(fileInfo);

    ChunkIndex chunkIdx = 0;
    RequestSourceInfo sourceInfo;

    // 第一个chunk
    sourceInfo =
        Splitor::CalcRequestSourceInfo(&ioTracker, &metaCache, chunkIdx);
    ASSERT_TRUE(sourceInfo.IsValid());
    ASSERT_EQ(sourceInfo.cloneFileSource, fileInfo.sourceInfo.name);
    ASSERT_EQ(sourceInfo.cloneFileOffset, 0);

    // 克隆卷最后一个chunk
    chunkIdx = fileInfo.sourceInfo.length / fileInfo.chunksize - 1;
    LOG(INFO) << "clone length = " << fileInfo.sourceInfo.length
              << ", chunk size = " << fileInfo.chunksize
              << ", chunk idx = " << chunkIdx;

    // offset = 10*1024*1024*1024 - 16 * 1024 * 1024 = 10720641024
    sourceInfo =
        Splitor::CalcRequestSourceInfo(&ioTracker, &metaCache, chunkIdx);
    ASSERT_TRUE(sourceInfo.IsValid());
    ASSERT_EQ(sourceInfo.cloneFileSource, fileInfo.sourceInfo.name);
    ASSERT_EQ(sourceInfo.cloneFileOffset, 10720641024);

    // 源卷未分配segment
    // 读取每个segment的第一个chunk
    for (int i = 1; i < 9; ++i) {
        ChunkIndex chunkIdx =
            i * cloneSourceInfo.segmentSize / fileInfo.chunksize;
        RequestSourceInfo sourceInfo = Splitor::CalcRequestSourceInfo(
            &ioTracker, &metaCache, chunkIdx);
        ASSERT_FALSE(sourceInfo.IsValid());
        ASSERT_TRUE(sourceInfo.cloneFileSource.empty());
        ASSERT_EQ(sourceInfo.cloneFileOffset, 0);
    }

    // 超过长度
    chunkIdx = fileInfo.sourceInfo.length / fileInfo.chunksize;

    sourceInfo =
        Splitor::CalcRequestSourceInfo(&ioTracker, &metaCache, chunkIdx);
    ASSERT_FALSE(sourceInfo.IsValid());
    ASSERT_TRUE(sourceInfo.cloneFileSource.empty());
    ASSERT_EQ(sourceInfo.cloneFileOffset, 0);

    // 源卷长度为0
    chunkIdx = 0;
    fileInfo.sourceInfo.length = 0;
    metaCache.UpdateFileInfo(fileInfo);
    sourceInfo =
        Splitor::CalcRequestSourceInfo(&ioTracker, &metaCache, chunkIdx);
    ASSERT_FALSE(sourceInfo.IsValid());
    ASSERT_TRUE(sourceInfo.cloneFileSource.empty());
    ASSERT_EQ(sourceInfo.cloneFileOffset, 0);

    // 不是read/write请求
    chunkIdx = 1;
    ioTracker.SetOpType(OpType::READ_SNAP);
    sourceInfo =
        Splitor::CalcRequestSourceInfo(&ioTracker, &metaCache, chunkIdx);
    ASSERT_FALSE(sourceInfo.IsValid());
    ASSERT_TRUE(sourceInfo.cloneFileSource.empty());
    ASSERT_EQ(sourceInfo.cloneFileOffset, 0);

    fileInfo.filestatus = FileStatus::Cloned;
    metaCache.UpdateFileInfo(fileInfo);

    chunkIdx = 0;

    // 不是克隆卷
    sourceInfo =
        Splitor::CalcRequestSourceInfo(&ioTracker, &metaCache, chunkIdx);
    ASSERT_FALSE(sourceInfo.IsValid());
    ASSERT_TRUE(sourceInfo.cloneFileSource.empty());
    ASSERT_EQ(sourceInfo.cloneFileOffset, 0);
}

TEST_F(IOTrackerSplitorTest, stripeTest) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    FInfo_t fi;
    uint64_t offset = 1 * 1024 * 1024 - 64 * 1024;
    uint64_t length = 128 * 1024;
    butil::IOBuf dataCopy;
    char* buf = new char[length];

    fi.seqnum = 0;
    fi.chunksize = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
    fi.stripeUnit = 1 * 1024 * 1024;
    fi.stripeCount = 4;
    memset(buf, 'a', length);              // 64KB
    dataCopy.append(buf, length);
    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = iomana->GetMetaCache();

    IOTracker* iotracker = new IOTracker(iomana, mc, &mockschuler);
    iotracker->SetOpType(OpType::WRITE);
    curve::client::ChunkIDInfo chinfo(1, 2, 3);
    curve::client::ChunkIDInfo chinfo1(4, 5, 6);
    mc->UpdateChunkInfoByIndex(0, chinfo);
    mc->UpdateChunkInfoByIndex(1, chinfo1);

    std::vector<RequestContext*> reqlist;
    ASSERT_EQ(0, curve::client::Splitor::IO2ChunkRequests(
                     iotracker, mc, &reqlist, &dataCopy, offset, length,
                     mdsclient_.get(), &fi));

    ASSERT_EQ(2, reqlist.size());
    RequestContext* first = reqlist.front();
    reqlist.erase(reqlist.begin());
    RequestContext* second = reqlist.front();
    reqlist.erase(reqlist.begin());

    ASSERT_EQ(1, first->idinfo_.cid_);
    ASSERT_EQ(3, first->idinfo_.cpid_);
    ASSERT_EQ(2, first->idinfo_.lpid_);
    ASSERT_EQ(1 * 1024 * 1024 - 64 * 1024, first->offset_);
    ASSERT_EQ(64 * 1024, first->rawlength_);

    ASSERT_EQ(4, second->idinfo_.cid_);
    ASSERT_EQ(6, second->idinfo_.cpid_);
    ASSERT_EQ(5, second->idinfo_.lpid_);
    ASSERT_EQ(0, second->offset_);
    ASSERT_EQ(64 * 1024, second->rawlength_);

    reqlist.clear();
    offset = 16 * 1024 * 1024 - 64 * 1024;
    length = 128 * 1024;
    memset(buf, 'b', length);
    dataCopy.append(buf, length);
    mc->UpdateChunkInfoByIndex(3, chinfo);
    mc->UpdateChunkInfoByIndex(4, chinfo1);
    ASSERT_EQ(0, curve::client::Splitor::IO2ChunkRequests(
                     iotracker, mc, &reqlist, &dataCopy, offset, length,
                     mdsclient_.get(), &fi));
    ASSERT_EQ(2, reqlist.size());
    first = reqlist.front();
    reqlist.erase(reqlist.begin());
    second = reqlist.front();
    reqlist.erase(reqlist.begin());

    ASSERT_EQ(1, first->idinfo_.cid_);
    ASSERT_EQ(3, first->idinfo_.cpid_);
    ASSERT_EQ(2, first->idinfo_.lpid_);
    ASSERT_EQ(4 * 1024 * 1024 - 64 * 1024, first->offset_);
    ASSERT_EQ(64 * 1024, first->rawlength_);

    ASSERT_EQ(4, second->idinfo_.cid_);
    ASSERT_EQ(6, second->idinfo_.cpid_);
    ASSERT_EQ(5, second->idinfo_.lpid_);
    ASSERT_EQ(0, second->offset_);
    ASSERT_EQ(64 * 1024, second->rawlength_);

    delete[] buf;
}

TEST_F(IOTrackerSplitorTest, TestDisableStripeForStripeFile) {
    MockRequestScheduler scheduler;
    scheduler.DelegateToFake();

    FInfo fi;
    fi.seqnum = 0;
    fi.chunksize = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
    fi.stripeUnit = 1 * 1024 * 1024;
    fi.stripeCount = 4;

    uint64_t offset = 1 * 1024 * 1024 - 64 * 1024;
    uint64_t length = 128 * 1024;
    std::unique_ptr<char[]> buf(new char[length]);
    memset(buf.get(), 'a', length);

    butil::IOBuf dataCopy;
    dataCopy.append(buf.get(), length);

    auto* iomanager = fileinstance_->GetIOManager4File();
    MetaCache* cache = iomanager->GetMetaCache();
    curve::client::ChunkIDInfo chinfo(1, 2, 3);
    curve::client::ChunkIDInfo chinfo1(4, 5, 6);
    cache->UpdateChunkInfoByIndex(0, chinfo);
    cache->UpdateChunkInfoByIndex(1, chinfo1);

    IOTracker ioTracker(iomanager, cache, &scheduler, nullptr, true);
    std::vector<RequestContext*> reqlist;
    ASSERT_EQ(0,
              Splitor::IO2ChunkRequests(&ioTracker, cache, &reqlist, &dataCopy,
                                        offset, length, mdsclient_.get(), &fi));

    ASSERT_EQ(2, reqlist.size());
    auto* first = reqlist[0];

    ASSERT_EQ(1, first->idinfo_.cid_);
    ASSERT_EQ(3, first->idinfo_.cpid_);
    ASSERT_EQ(2, first->idinfo_.lpid_);
    ASSERT_EQ(1 * 1024 * 1024 - 64 * 1024, first->offset_);
    ASSERT_EQ(length / 2, first->rawlength_);

    auto* second = reqlist[1];
    ASSERT_EQ(1, second->idinfo_.cid_);
    ASSERT_EQ(3, second->idinfo_.cpid_);
    ASSERT_EQ(2, second->idinfo_.lpid_);
    ASSERT_EQ(1 * 1024 * 1024, second->offset_);
    ASSERT_EQ(length / 2, second->rawlength_);
}

// read the chunks all haven't been write from normal volume with no clonesource
TEST_F(IOTrackerSplitorTest, StartReadNotAllocateSegment) {
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(notallocatefakeret);
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    iomana->SetRequestScheduler(mockschuler);

    uint64_t offset = 1 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    auto threadfunc = [&]() {
        iomana->Read(data, offset, length, mdsclient_.get());
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    for (int i = 0; i < length; i++) {
       ASSERT_EQ(0, data[i]);
    }
    delete[] data;
}

TEST_F(IOTrackerSplitorTest, AsyncStartReadNotAllocateSegment) {
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(notallocatefakeret);
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    iomana->SetRequestScheduler(mockschuler);

    CurveAioContext aioctx;
    aioctx.offset = 1 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = readcallback;
    aioctx.buf = new char[aioctx.length];
    aioctx.op = LIBCURVE_OP::LIBCURVE_OP_READ;

    ioreadflag = false;
    char* data = static_cast<char*>(aioctx.buf);
    iomana->AioRead(&aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(readmtx);
        readcv.wait(lk, []()->bool{return ioreadflag;});
    }

    for (int i = 0; i < aioctx.length; i++) {
       ASSERT_EQ(0, data[i]);
    }
    delete[] data;
}

// read the chunks some of them haven't been writtern from normal volume
// with no clonesource
TEST_F(IOTrackerSplitorTest, StartReadNotAllocateSegment2) {
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(notallocatefakeret);
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    ChunkIDInfo chunkIdInfo(1, 1, 256);
    mc->UpdateChunkInfoByIndex(256, chunkIdInfo);
    iomana->SetRequestScheduler(mockschuler);

    uint64_t offset = 1 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    auto threadfunc = [&]() {
        iomana->Read(data, offset, length, mdsclient_.get());
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    for (int i = 0; i < 4 * 1024; i++) {
        ASSERT_EQ('a', data[i]);
    }

    for (int i = 4 * 1024; i < length; i++) {
        ASSERT_EQ(0, data[i]);
    }
    delete[] data;
}

TEST_F(IOTrackerSplitorTest, AsyncStartReadNotAllocateSegment2) {
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(notallocatefakeret);
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    ChunkIDInfo chunkIdInfo(1, 1, 256);
    mc->UpdateChunkInfoByIndex(256, chunkIdInfo);
    iomana->SetRequestScheduler(mockschuler);

    CurveAioContext aioctx;
    aioctx.offset = 1 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = readcallback;
    aioctx.buf = new char[aioctx.length];
    aioctx.op = LIBCURVE_OP::LIBCURVE_OP_READ;

    ioreadflag = false;
    char* data = static_cast<char*>(aioctx.buf);
    iomana->AioRead(&aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(readmtx);
        readcv.wait(lk, []()->bool{return ioreadflag;});
    }

    for (int i = 0; i < 4 * 1024; i++) {
        ASSERT_EQ('a', data[i]);
    }

    for (int i = 4 * 1024; i < aioctx.length; i++) {
        ASSERT_EQ(0, data[i]);
    }
    delete[] data;
}

// read the chunks some haven't been write from clone volume with clonesource
TEST_F(IOTrackerSplitorTest, StartReadNotAllocateSegmentFromOrigin) {
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(notallocatefakeret);
    curvefsservice.SetGetOrAllocateSegmentFakeReturnForClone
                                                (getsegmentfakeretclone);
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    FileInstance* fileinstance2 = new FileInstance();
    userinfo.owner = "cloneuser-test1";
    userinfo.password = "12345";
    mdsclient_->Initialize(fopt.metaServerOpt);
    fileinstance2->Initialize("/clonesource", mdsclient_, userinfo, OpenFlags{},
                              fopt);

    MockRequestScheduler* mockschuler2 = new MockRequestScheduler;
    mockschuler2->DelegateToFake();

    fileinstance2->GetIOManager4File()->SetRequestScheduler(mockschuler2);

    auto& handlers = SourceReader::GetInstance().GetReadHandlers();
    handlers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple("/clonesource"),
        std::forward_as_tuple(fileinstance2, ::time(nullptr), false));

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    ChunkIDInfo chunkIdInfo(1, 1, 257);
    mc->UpdateChunkInfoByIndex(257, chunkIdInfo);

    FInfo_t fileInfo;
    fileInfo.chunksize = 4 * 1024 * 1024;               // 4M
    fileInfo.fullPathName = "/1_userinfo_.txt";
    fileInfo.owner = "userinfo";
    fileInfo.filestatus = FileStatus::CloneMetaInstalled;
    fileInfo.sourceInfo.name = "/clonesource";
    fileInfo.sourceInfo.segmentSize = 1ull * 1024 * 1024 * 1024;
    fileInfo.sourceInfo.length = 10ull * 1024 * 1024 * 1024;
    for (uint64_t i = 0; i < fileInfo.sourceInfo.length;
         i += fileInfo.sourceInfo.segmentSize) {
        fileInfo.sourceInfo.allocatedSegmentOffsets.insert(i);
    }
    fileInfo.userinfo = userinfo;
    mc->UpdateFileInfo(fileInfo);

    iomana->SetRequestScheduler(mockschuler);

    uint64_t offset = 1 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    auto threadfunc = [&]() {
        iomana->Read(data, offset, length, mdsclient_.get());
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    LOG(ERROR) << "address = " << &data;
    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('a', data[4 * 1024]);
    ASSERT_EQ('d', data[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('a', data[4 * 1024 + chunk_size]);
    ASSERT_EQ('a', data[length - 1]);


    fileinstance2->UnInitialize();
    delete fileinstance2;

    delete[] data;
}

TEST_F(IOTrackerSplitorTest, AsyncStartReadNotAllocateSegmentFromOrigin) {
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(notallocatefakeret);
    curvefsservice.SetGetOrAllocateSegmentFakeReturnForClone
                                                (getsegmentfakeretclone);
    MockRequestScheduler* mockschuler = new MockRequestScheduler;
    mockschuler->DelegateToFake();

    FileInstance* fileinstance2 = new FileInstance();
    userinfo.owner = "cloneuser-test2";
    userinfo.password = "12345";
    mdsclient_->Initialize(fopt.metaServerOpt);
    fileinstance2->Initialize("/clonesource", mdsclient_, userinfo, OpenFlags{},
                              fopt);

    MockRequestScheduler* mockschuler2 = new MockRequestScheduler;
    mockschuler2->DelegateToFake();

    fileinstance2->GetIOManager4File()->SetRequestScheduler(mockschuler2);

    auto& handlers = SourceReader::GetInstance().GetReadHandlers();
    handlers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple("/clonesource"),
        std::forward_as_tuple(fileinstance2, ::time(nullptr), false));

    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    MetaCache* mc = fileinstance_->GetIOManager4File()->GetMetaCache();
    ChunkIDInfo chunkIdInfo(1, 1, 257);
    mc->UpdateChunkInfoByIndex(257, chunkIdInfo);

    FInfo_t fileInfo;
    fileInfo.chunksize = 4 * 1024 * 1024;
    fileInfo.filename = "1_userinfo_.txt";
    fileInfo.owner = "userinfo";
    fileInfo.filestatus = FileStatus::CloneMetaInstalled;
    fileInfo.sourceInfo.name = "/clonesource";
    fileInfo.sourceInfo.segmentSize = 1ull * 1024 * 1024 * 1024;
    fileInfo.sourceInfo.length = 10ull * 1024 * 1024 * 1024;
    for (uint64_t i = 0; i < fileInfo.sourceInfo.length;
         i += fileInfo.sourceInfo.segmentSize) {
        fileInfo.sourceInfo.allocatedSegmentOffsets.insert(i);
    }
    fileInfo.userinfo = userinfo;
    mc->UpdateFileInfo(fileInfo);

    iomana->SetRequestScheduler(mockschuler);

    CurveAioContext aioctx;
    aioctx.offset = 1 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = readcallback;
    aioctx.buf = new char[aioctx.length];
    aioctx.op = LIBCURVE_OP::LIBCURVE_OP_READ;

    ioreadflag = false;
    char* data = static_cast<char*>(aioctx.buf);
    iomana->AioRead(&aioctx, mdsclient_.get(), UserDataType::RawBuffer);

    {
        std::unique_lock<std::mutex> lk(readmtx);
        readcv.wait(lk, []()->bool{return ioreadflag;});
    }
    LOG(ERROR) << "address = " << &data;
    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('a', data[4 * 1024]);
    ASSERT_EQ('d', data[4 * 1024 + chunk_size - 1]);
    ASSERT_EQ('a', data[4 * 1024 + chunk_size]);
    ASSERT_EQ('a', data[aioctx.length - 1]);

    fileinstance2->UnInitialize();
    delete fileinstance2;
    delete[] data;
}

TEST_F(IOTrackerSplitorTest, TimedCloseFd) {
    std::unordered_map<std::string, SourceReader::ReadHandler> fakeHandlers;
    fakeHandlers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple("/1"),
        std::forward_as_tuple(
            nullptr,
            ::time(nullptr) - fopt.ioOpt.closeFdThreadOption.fdTimeout,
            true));
    fakeHandlers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple("/2"),
        std::forward_as_tuple(
            nullptr,
            ::time(nullptr) - fopt.ioOpt.closeFdThreadOption.fdTimeout,
            false));

    FileInstance* instance = new FileInstance();
    fakeHandlers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple("/3"),
        std::forward_as_tuple(
            instance,
            ::time(nullptr) - fopt.ioOpt.closeFdThreadOption.fdTimeout,
            false));

    SourceReader::GetInstance().SetReadHandlers(fakeHandlers);

    std::this_thread::sleep_for(std::chrono::seconds(30));
    SourceReader::GetInstance().Stop();
    ASSERT_EQ(0, SourceReader::GetInstance().GetReadHandlers().size());
}

}  // namespace client
}  // namespace curve
