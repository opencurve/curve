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
 * Date: Tue Jun 22 19:26:16 CST 2021
 * Author: wuhanqing
 */

#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gtest/gtest.h>

#include "src/client/client_common.h"
#include "src/client/io_tracker.h"
#include "src/client/splitor.h"
#include "test/client/mock_meta_cache.h"
#include "test/client/mock/mock_chunk_service.h"
#include "test/client/mock_request_scheduler.h"

namespace curve {
namespace client {

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;

using ::testing::AllOf;
using ::testing::AtLeast;
using ::testing::Ge;
using ::testing::Le;
using ::testing::Matcher;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::SetArgPointee;
using ::testing::SaveArgPointee;

struct SaveReadRequst {
    explicit SaveReadRequst(curve::chunkserver::ChunkRequest* r) : r_(r) {}

    void operator()(::google::protobuf::RpcController* cntl_base,
                    const chunkserver::ChunkRequest* request,
                    chunkserver::ChunkResponse* response,
                    google::protobuf::Closure* done) {
        LOG(INFO) << "SaveReadRequest";
        LOG(INFO) << request->DebugString();

        r_->CopyFrom(*request);

        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->response_attachment().resize(request->size(), 'x');
        response->set_status(
            chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        done->Run();
    }

    curve::chunkserver::ChunkRequest* r_;
};

struct SaveWriteData {
    explicit SaveWriteData(butil::IOBuf* data) : data_(data) {}

    void operator()(::google::protobuf::RpcController* cntl_base,
                    const chunkserver::ChunkRequest* request,
                    chunkserver::ChunkResponse* response,
                    google::protobuf::Closure* done) {
        LOG(INFO) << "SaveWriteData";

        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        *data_ = cntl->request_attachment();

        response->set_status(
            chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        done->Run();
    }

    butil::IOBuf* data_;
};

class IOTrackerAlignmentTest : public ::testing::Test {
    void SetUp() override {
        Splitor::Init(splitOpt_);

        mdsClient_.reset(new MDSClient());
        mockMetaCache_.reset(new MockMetaCache());

        MetaCacheOption metaCacheOpt;
        mockMetaCache_->Init(metaCacheOpt, mdsClient_.get());

        fileInfo_.fullPathName = "/IOTrackerAlignmentTest";
        fileInfo_.length = 100 * GiB;
        fileInfo_.segmentsize = 1 * GiB;
        fileInfo_.chunksize = 16 * MiB;
        fileInfo_.filestatus = FileStatus::CloneMetaInstalled;

        mockMetaCache_->UpdateFileInfo(fileInfo_);

        ASSERT_EQ(0, scheduler_.Init({}, mockMetaCache_.get()));
        ASSERT_EQ(0, scheduler_.Run());

        mockChunkService_.reset(new chunkserver::MockChunkService());

        ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:21001", &csEp_));
        ASSERT_EQ(0, server_.AddService(mockChunkService_.get(),
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(csEp_, nullptr));
    }

    void TearDown() override {
        scheduler_.Fini();

        server_.Stop(0);
        server_.Join();
    }

 protected:
    FInfo fileInfo_;
    std::unique_ptr<MockMetaCache> mockMetaCache_;
    std::unique_ptr<MDSClient> mdsClient_;
    std::unique_ptr<chunkserver::MockChunkService> mockChunkService_;

    RequestScheduler scheduler_;

    IOSplitOption splitOpt_;
    brpc::Server server_;
    ChunkServerID csId_;
    butil::EndPoint csEp_;
    FileMetric metric_{"/IOTrackerAlignmentTest"};
};

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite1) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    curve::chunkserver::ChunkRequest readRequest;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .WillOnce(Invoke(SaveReadRequst(&readRequest)));

    butil::IOBuf writeData;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .WillOnce(Invoke(SaveWriteData(&writeData)));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 0;
    uint64_t length = 2048;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(0, readRequest.offset());
    EXPECT_EQ(4096, readRequest.size());
    EXPECT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequest.optype());

    butil::IOBuf expected;
    expected.resize(2048, 'a');
    expected.resize(4096, 'x');
    EXPECT_EQ(writeData, expected);
}

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite2) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    curve::chunkserver::ChunkRequest readRequest;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .WillOnce(Invoke(SaveReadRequst(&readRequest)));

    butil::IOBuf writeData;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .WillOnce(Invoke(SaveWriteData(&writeData)));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 0;
    uint64_t length = 64 * KiB - 1024;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(60 * KiB, readRequest.offset());
    EXPECT_EQ(4096, readRequest.size());
    EXPECT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequest.optype());

    butil::IOBuf expected;
    expected.resize(length, 'a');
    expected.resize(length + 1024, 'x');
    EXPECT_EQ(writeData, expected);
}

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite3) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    curve::chunkserver::ChunkRequest readRequest;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .WillOnce(Invoke(SaveReadRequst(&readRequest)));

    butil::IOBuf writeData;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .WillOnce(Invoke(SaveWriteData(&writeData)));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 2048;
    uint64_t length = 2048;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(0, readRequest.offset());
    EXPECT_EQ(4096, readRequest.size());
    EXPECT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequest.optype());

    butil::IOBuf expected;
    expected.resize(2048, 'x');
    expected.resize(4096, 'a');
    EXPECT_EQ(writeData, expected);
}

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite4) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    curve::chunkserver::ChunkRequest readRequest;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .WillOnce(Invoke(SaveReadRequst(&readRequest)));

    butil::IOBuf writeData;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .WillOnce(Invoke(SaveWriteData(&writeData)));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 1024;
    uint64_t length = 64 * KiB - 1024;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(0, readRequest.offset());
    EXPECT_EQ(4096, readRequest.size());
    EXPECT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequest.optype());

    butil::IOBuf expected;
    expected.resize(1024, 'x');
    expected.resize(length + 1024, 'a');
    EXPECT_EQ(writeData, expected);
}

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite5) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    curve::chunkserver::ChunkRequest readRequest;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .WillOnce(Invoke(SaveReadRequst(&readRequest)));

    butil::IOBuf writeData;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .WillOnce(Invoke(SaveWriteData(&writeData)));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 1024;
    uint64_t length = 2048;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(0, readRequest.offset());
    EXPECT_EQ(4096, readRequest.size());
    EXPECT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequest.optype());

    butil::IOBuf expected;
    expected.resize(1024, 'x');
    expected.resize(3072, 'a');
    expected.resize(4096, 'x');
    EXPECT_EQ(writeData, expected);
}

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite6) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    curve::chunkserver::ChunkRequest readRequest;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .WillOnce(Invoke(SaveReadRequst(&readRequest)));

    butil::IOBuf writeData;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .WillOnce(Invoke(SaveWriteData(&writeData)));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 3072;
    uint64_t length = 2048;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(0, readRequest.offset());
    EXPECT_EQ(8192, readRequest.size());
    EXPECT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequest.optype());

    butil::IOBuf expected;
    expected.resize(3072, 'x');
    expected.resize(3072 + 2048, 'a');
    expected.resize(8192, 'x');
    EXPECT_EQ(writeData, expected);
}

TEST_F(IOTrackerAlignmentTest, TestUnalignedWrite7) {
    EXPECT_CALL(*mockMetaCache_, GetChunkInfoByIndex(_, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke([](ChunkIndex idx, ChunkIDInfo_t* info) {
            info->chunkExist = true;
            info->lpid_ = 1;
            info->cpid_ = 2;
            info->cid_ = 3;

            return MetaCacheErrorType::OK;
        }));

    EXPECT_CALL(*mockMetaCache_, GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(csId_), SetArgPointee<3>(csEp_), Return(0)));

    std::vector<curve::chunkserver::ChunkRequest> readRequests;
    std::mutex mtx;
    EXPECT_CALL(*mockChunkService_, ReadChunk(_, _, _, _))
        .Times(2)
        .WillRepeatedly(Invoke([&readRequests, &mtx](
                                   ::google::protobuf::RpcController* cntl_base,
                                   const chunkserver::ChunkRequest* request,
                                   chunkserver::ChunkResponse* response,
                                   google::protobuf::Closure* done) {
            {
                std::lock_guard<std::mutex> lock(mtx);
                readRequests.push_back(*request);
            }

            brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
            cntl->response_attachment().resize(request->size(), 'x');
            response->set_status(
                chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            done->Run();
        }));

    std::vector<butil::IOBuf> writeDatas;
    std::mutex mtx2;
    EXPECT_CALL(*mockChunkService_, WriteChunk(_, _, _, _))
        .Times(2)
        .WillRepeatedly(Invoke(
            [&writeDatas, &mtx2](::google::protobuf::RpcController* cntl_base,
                                 const chunkserver::ChunkRequest* request,
                                 chunkserver::ChunkResponse* response,
                                 google::protobuf::Closure* done) {
                {
                    brpc::Controller* cntl =
                        static_cast<brpc::Controller*>(cntl_base);
                    std::lock_guard<std::mutex> lock(mtx2);
                    writeDatas.push_back(cntl->request_attachment());
                }

                response->set_status(
                    chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
                done->Run();
            }));

    IOTracker tracker(nullptr, mockMetaCache_.get(), &scheduler_, &metric_);

    butil::IOBuf fakeData;
    uint64_t offset = 2048;
    uint64_t length = 60 * KiB;
    fakeData.resize(length, 'a');
    tracker.SetUserDataType(UserDataType::IOBuffer);
    tracker.StartWrite(&fakeData, offset, length, mdsClient_.get(),
                       &fileInfo_, nullptr);

    EXPECT_GE(tracker.Wait(), 0);

    EXPECT_EQ(2, readRequests.size());
    EXPECT_EQ(2, writeDatas.size());

    if (0 == readRequests[0].offset()) {
        ASSERT_EQ(0, readRequests[0].offset());
        ASSERT_EQ(4096, readRequests[0].size());
        ASSERT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequests[0].optype());

        ASSERT_EQ(60 * KiB, readRequests[1].offset());
        ASSERT_EQ(4096, readRequests[1].size());
        ASSERT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequests[1].optype());
    } else {
        ASSERT_EQ(0, readRequests[1].offset());
        ASSERT_EQ(4096, readRequests[1].size());
        ASSERT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequests[1].optype());

        ASSERT_EQ(60 * KiB, readRequests[0].offset());
        ASSERT_EQ(4096, readRequests[0].size());
        ASSERT_EQ(curve::chunkserver::CHUNK_OP_READ, readRequests[0].optype());
    }

    if (writeDatas[0].size() == 60 *KiB) {
        butil::IOBuf expected;
        expected.resize(2048, 'x');
        expected.resize(60 * KiB, 'a');

        ASSERT_EQ(expected, writeDatas[0]);

        expected.clear();
        expected.resize(2048, 'a');
        expected.resize(4096, 'x');
        ASSERT_EQ(expected, writeDatas[1]);
    } else {
        butil::IOBuf expected;
        expected.resize(2048, 'x');
        expected.resize(60 * KiB, 'a');

        ASSERT_EQ(expected, writeDatas[1]);

        expected.clear();
        expected.resize(2048, 'a');
        expected.resize(4096, 'x');
        ASSERT_EQ(expected, writeDatas[0]);
    }

    // butil::IOBuf expected;
    // expected.resize(2048, 'x');
    // expected.resize(60 * KiB + 2048, 'a');
    // expected.resize(64 * KiB, 'x');
    // EXPECT_EQ(writeData, expected);
}

}  // namespace client
}  // namespace curve
