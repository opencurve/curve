/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:17:04 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>

#include <string>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT

#include "test/client/fake/mock_schedule.h"
#include "src/client/io_context.h"
#include "src/client/context_slab.h"
#include "src/client/splitor.h"
#include "test/client/fake/mockMDS.h"
#include "src/client/client_common.h"
#include "src/client/session.h"
#include "src/client/metacache.h"
#include "src/client/io_context_manager.h"
#include "include/client/libcurve.h"

DECLARE_string(metaserver_addr);
DECLARE_uint32(chunk_size);

extern char* writebuffer;

using curve::client::Session;
using curve::client::IOContext;
using curve::client::MetaCache;
using curve::client::RequestContext;
using curve::client::IOContextSlab;
using curve::client::IOContextManager;
using curve::client::RequestContextSlab;

void callback(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->err;
}

class IOContextSplitorTest : public ::testing::Test {
 public:
    void SetUp() {
        session_ = new Session();
        session_->Initialize();
        InsertMetaCache();

        iocontextslab = new IOContextSlab();
        requestslab = new RequestContextSlab();

        iocontextslab->Initialize();
        requestslab->Initialize();
    }

    void TearDown() {
        server.Stop(0);
        server.Join();
        session_->UnInitialize();

        iocontextslab->UnInitialize();
        requestslab->UnInitialize();
        delete iocontextslab;
        delete requestslab;
        delete session_;
    }

    void InsertMetaCache() {
        FakeCurveFSService curvefsservice;
        FakeTopologyService topologyservice;

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
        if (server.Start(FLAGS_metaserver_addr.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to start Server";
        }

        curve::mds::GetOrAllocateSegmentResponse response;
        curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;

        response.set_statuscode(::curve::mds::StatusCode::kOK);
        response.set_allocated_pagefilesegment(pfs);
        response.mutable_pagefilesegment()->
            set_logicalpoolid(1234);
        response.mutable_pagefilesegment()->
            set_segmentsize(1 * 1024 * 1024 * 1024);
        response.mutable_pagefilesegment()->
            set_chunksize(4 * 1024 * 1024);
        response.mutable_pagefilesegment()->
            set_startoffset(0);

        for (int i = 0; i < 256; i ++) {
            auto chunk = response.mutable_pagefilesegment()->add_chunks();
            chunk->set_copysetid(i);
            chunk->set_chunkid(i);
        }
        FakeReturn* fakeret = new FakeReturn(nullptr,
                    static_cast<void*>(&response));
        curvefsservice.SetFakeReturn(fakeret);

        ::curve::mds::topology::GetChunkServerListInCopySetsResponse response_1;
        response_1.set_statuscode(0);
        uint64_t chunkserveridc = 1;
        for (int i = 0; i < 256; i ++) {
            auto csinfo = response_1.add_csinfo();
            csinfo->set_copysetid(i);

            for (int j = 0; j < 3; j++) {
                auto cslocs = csinfo->add_cslocs();
                cslocs->set_chunkserverid(chunkserveridc++);
                cslocs->set_hostip("127.0.0.1");
                cslocs->set_port(8000);
            }
        }
        FakeReturn* faktopologyeret = new FakeReturn(nullptr,
                    static_cast<void*>(&response_1));
        topologyservice.SetFakeReturn(faktopologyeret);

        session_->GetOrAllocateSegment(0);

        delete fakeret;
        delete faktopologyeret;
    }

    Session*    session_;
    brpc::Server server;
    IOContextSlab* iocontextslab;
    RequestContextSlab* requestslab;
};

TEST_F(IOContextSplitorTest, AsyncStartRead) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    IOContext* iocontext = iocontextslab->Get();
    iocontext->SetScheduler(&mockschuler);

    ASSERT_NE(nullptr, iocontext);
    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = 0;
    aioctx.err = LIBCURVE_ERROR_UNKNOWN;
    aioctx.cb = callback;
    aioctx.buf = new char[aioctx.length];

    MetaCache* mc = session_->GetMetaCache();

    char* data = static_cast<char*>(aioctx.buf);
    iocontext->StartRead(&aioctx,
                        mc,
                        requestslab,
                        data,
                        aioctx.offset,
                        aioctx.length);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('b', data[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', data[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', data[aioctx.length - 1]);

    delete[] data;
}

TEST_F(IOContextSplitorTest, AsyncStartWrite) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    IOContext* iocontext = iocontextslab->Get();
    iocontext->SetScheduler(&mockschuler);

    ASSERT_NE(nullptr, iocontext);
    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = 0;
    aioctx.err = LIBCURVE_ERROR_UNKNOWN;
    aioctx.cb = callback;
    aioctx.buf = new char[aioctx.length];

    MetaCache* mc = session_->GetMetaCache();

    char* data = static_cast<char*>(aioctx.buf);

    memset(data, 'a', 4 * 1024);
    memset(data + 4 * 1024, 'b', FLAGS_chunk_size);
    memset(data + 4 * 1024 + FLAGS_chunk_size, 'c', 4 * 1024);

    iocontext->StartWrite(&aioctx,
                        mc,
                        requestslab,
                        data,
                        aioctx.offset,
                        aioctx.length);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', writebuffer[aioctx.length - 1]);

    delete[] data;
}

TEST_F(IOContextSplitorTest, StartRead) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    IOContext* iocontext = iocontextslab->Get();
    iocontext->SetScheduler(&mockschuler);

    ASSERT_NE(nullptr, iocontext);
    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    MetaCache* mc = session_->GetMetaCache();

    auto waitfunc = [&]() {
        uint32_t retlen = iocontext->Wait();
        ASSERT_EQ(length, retlen);
    };

    auto threadfunc = [&]() {
        iocontext->StartRead(nullptr,
                            mc,
                            requestslab,
                            data,
                            offset,
                            length);
    };
    std::thread waitthread(waitfunc);
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }
    if (waitthread.joinable()) {
        waitthread.join();
    }

    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('b', data[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', data[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', data[length - 1]);

    delete[] data;
}

TEST_F(IOContextSplitorTest, StartWrite) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    IOContext* iocontext = iocontextslab->Get();
    iocontext->SetScheduler(&mockschuler);

    ASSERT_NE(nullptr, iocontext);
    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    memset(buf, 'a', 4 * 1024);
    memset(buf + 4 * 1024, 'b', FLAGS_chunk_size);
    memset(buf + 4 * 1024 + FLAGS_chunk_size, 'c', 4 * 1024);

    MetaCache* mc = session_->GetMetaCache();

    auto waitfunc = [&]() {
        uint32_t retlen = iocontext->Wait();
        ASSERT_EQ(length, retlen);
    };

    auto threadfunc = [&]() {
        iocontext->StartWrite(nullptr,
                            mc,
                            requestslab,
                            buf,
                            offset,
                            length);
    };
    std::thread process(threadfunc);
    std::thread waitthread(waitfunc);

    if (process.joinable()) {
        process.join();
    }
    if (waitthread.joinable()) {
        waitthread.join();
    }

    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', writebuffer[length - 1]);

    delete[] buf;
}


TEST_F(IOContextSplitorTest, ManagerAsyncStartRead) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    MetaCache* mc = session_->GetMetaCache();
    auto ioctxmana = new IOContextManager(mc, &mockschuler);
    ASSERT_TRUE(ioctxmana->Initialize());

    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = 0;
    aioctx.err = LIBCURVE_ERROR_UNKNOWN;
    aioctx.cb = callback;
    aioctx.buf = new char[aioctx.length];

    char* data = static_cast<char*>(aioctx.buf);
    ioctxmana->AioRead(&aioctx);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('b', data[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', data[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', data[aioctx.length - 1]);

    delete[] data;
}

TEST_F(IOContextSplitorTest, ManagerAsyncStartWrite) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    MetaCache* mc = session_->GetMetaCache();
    auto ioctxmana = new IOContextManager(mc, &mockschuler);
    ASSERT_TRUE(ioctxmana->Initialize());

    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = 0;
    aioctx.err = LIBCURVE_ERROR_UNKNOWN;
    aioctx.cb = callback;
    aioctx.buf = new char[aioctx.length];

    char* data = static_cast<char*>(aioctx.buf);

    memset(data, 'a', 4 * 1024);
    memset(data + 4 * 1024, 'b', FLAGS_chunk_size);
    memset(data + 4 * 1024 + FLAGS_chunk_size, 'c', 4 * 1024);

    ioctxmana->AioWrite(&aioctx);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', writebuffer[aioctx.length - 1]);

    delete[] data;
}

TEST_F(IOContextSplitorTest, ManagerStartRead) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    MetaCache* mc = session_->GetMetaCache();
    auto ioctxmana = new IOContextManager(mc, &mockschuler);
    ASSERT_TRUE(ioctxmana->Initialize());

    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* data = new char[length];

    auto threadfunc = [&]() {
        ASSERT_EQ(length, ioctxmana->Read(data,
                                        offset,
                                        length));
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    ASSERT_EQ('a', data[0]);
    ASSERT_EQ('a', data[4 * 1024 - 1]);
    ASSERT_EQ('b', data[4 * 1024]);
    ASSERT_EQ('b', data[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', data[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', data[length - 1]);

    delete[] data;
}

TEST_F(IOContextSplitorTest, ManagerStartWrite) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    MetaCache* mc = session_->GetMetaCache();
    auto ioctxmana = new IOContextManager(mc, &mockschuler);
    ASSERT_TRUE(ioctxmana->Initialize());

    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    memset(buf, 'a', 4 * 1024);
    memset(buf + 4 * 1024, 'b', FLAGS_chunk_size);
    memset(buf + 4 * 1024 + FLAGS_chunk_size, 'c', 4 * 1024);

    auto threadfunc = [&]() {
        ioctxmana->Write(buf,
                        offset,
                        length);
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    ASSERT_EQ('a', writebuffer[0]);
    ASSERT_EQ('a', writebuffer[4 * 1024 - 1]);
    ASSERT_EQ('b', writebuffer[4 * 1024]);
    ASSERT_EQ('b', writebuffer[4 * 1024 + FLAGS_chunk_size - 1]);
    ASSERT_EQ('c', writebuffer[4 * 1024 + FLAGS_chunk_size]);
    ASSERT_EQ('c', writebuffer[length - 1]);

    delete[] buf;
}

TEST_F(IOContextSplitorTest, ExceptionTest_TEST) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    auto session = new Session();
    ASSERT_TRUE(session->Initialize());

    auto iocontextslab = new IOContextSlab();
    auto requestslab = new RequestContextSlab();

    iocontextslab->Initialize();
    requestslab->Initialize();

    IOContext* iocontext = iocontextslab->Get();
    iocontext->SetScheduler(&mockschuler);
    ASSERT_NE(nullptr, iocontext);
    uint64_t offset = 4 * 1024 * 1024 - 4 * 1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];
    MetaCache* mc = session->GetMetaCache();

    auto waitfunc = [&]() {
        uint32_t retlen = iocontext->Wait();
        ASSERT_EQ(-1, retlen);
    };

    auto threadfunc = [&]() {
        iocontext->StartWrite(nullptr,
                            nullptr,
                            requestslab,
                            buf,
                            offset,
                            length);
    };
    std::thread process(threadfunc);
    std::thread waitthread(waitfunc);

    if (process.joinable()) {
        process.join();
    }
    if (waitthread.joinable()) {
        waitthread.join();
    }

    iocontextslab->UnInitialize();
    requestslab->UnInitialize();
    delete iocontextslab;
    delete requestslab;
    delete session;
}

TEST_F(IOContextSplitorTest, BoundaryTEST) {
    MockRequestScheduler mockschuler;
    mockschuler.DelegateToFake();

    MetaCache* mc = session_->GetMetaCache();
    auto ioctxmana = new IOContextManager(mc, &mockschuler);
    ASSERT_TRUE(ioctxmana->Initialize());

    /**
     * this offset and length will make splitor split fail.
     * we set disk size = 1G.
     */ 
    uint64_t offset = 1 * 1024 * 1024 * 1024 -
                     4 * 1024 * 1024 - 4 *1024;
    uint64_t length = 4 * 1024 * 1024 + 8 * 1024;
    char* buf = new char[length];

    memset(buf, 'a', 4 * 1024);
    memset(buf + 4 * 1024, 'b', FLAGS_chunk_size);
    memset(buf + 4 * 1024 + FLAGS_chunk_size, 'c', 4 * 1024);

    auto threadfunc = [&]() {
        ASSERT_EQ(-1, ioctxmana->Write(buf,
                                    offset,
                                    length));
    };
    std::thread process(threadfunc);

    if (process.joinable()) {
        process.join();
    }

    delete[] buf;
}
