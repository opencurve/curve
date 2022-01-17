/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Sat Sep  4 19:43:01 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_service.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gtest/gtest.h>

#include "curvefs/proto/copyset.pb.h"
#include "src/common/uuid.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::UUIDGenerator;
using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFsFactory;

const std::string kTestDataPath = "./runlog/" + UUIDGenerator{}.GenerateUUID();  // NOLINT
const std::string kTestTrashPath = kTestDataPath + "_trash";  // NOLINT

const char* kTestIp = "127.0.0.1";
const int kTestPort = 29960;

class CopysetServiceTest : public testing::Test {
 protected:
    void SetUp() override {
        poolId_ = time(nullptr) % 123456;
        copysetId_ = time(nullptr) % reinterpret_cast<uint64_t>(this);

        fs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        options_.dataUri = "local://" + kTestDataPath;
        options_.raftNodeOptions.log_uri = "local://" + kTestDataPath;
        options_.raftNodeOptions.raft_meta_uri = "local://" + kTestDataPath;
        options_.raftNodeOptions.snapshot_uri = "local://" + kTestDataPath;

        options_.ip = kTestIp;
        options_.port = kTestPort;

        options_.localFileSystem = fs_.get();

        options_.trashOptions.trashUri = "local://" + kTestTrashPath;

        butil::EndPoint listenAddr(butil::IP_ANY, kTestPort);

        nodeManager_->AddService(&server_, listenAddr);

        ASSERT_TRUE(nodeManager_->Init(options_));
        ASSERT_TRUE(nodeManager_->Start());

        ASSERT_EQ(0, server_.AddService(new CopysetServiceImpl(nodeManager_),
                                        brpc::SERVER_OWNS_SERVICE));
        ASSERT_EQ(0, server_.Start(listenAddr, nullptr));

        brpc::ChannelOptions opts;
        opts.timeout_ms = 10 * 1000;
        opts.max_retry = 0;
        ASSERT_EQ(0, channel_.Init(listenAddr, &opts));
    }

    void TearDown() override {
        nodeManager_->Stop();

        server_.Stop(0);
        server_.Join();

        system(std::string("rm -rf " + kTestDataPath).c_str());
    }

 protected:
    CopysetNodeOptions options_;
    std::shared_ptr<LocalFileSystem> fs_;
    brpc::Server server_;
    brpc::Channel channel_;
    PoolId poolId_;
    CopysetId copysetId_;
    CopysetNodeManager* nodeManager_ = &CopysetNodeManager::GetInstance();
};

TEST_F(CopysetServiceTest, CreateCopysetTest) {
    // create one copyset
    {
        CopysetService_Stub stub(&channel_);
        brpc::Controller cntl;

        CreateCopysetRequest request;
        CreateCopysetResponse response;

        auto* copyset = request.add_copysets();
        copyset->set_poolid(poolId_);
        copyset->set_copysetid(copysetId_);
        copyset->add_peers()->set_address("127.0.0.1:29960:0");
        copyset->add_peers()->set_address("127.0.0.1:29961:0");
        copyset->add_peers()->set_address("127.0.0.1:29962:0");

        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS,
                  response.status());
    }

    // create duplicate copyset
    {
        CopysetService_Stub stub(&channel_);
        brpc::Controller cntl;

        CreateCopysetRequest request;
        CreateCopysetResponse response;

        auto* copyset = request.add_copysets();
        copyset->set_poolid(poolId_);
        copyset->set_copysetid(copysetId_);
        copyset->add_peers()->set_address("127.0.0.1:29960:0");
        copyset->add_peers()->set_address("127.0.0.1:29961:0");
        copyset->add_peers()->set_address("127.0.0.1:29962:0");

        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS,
                  response.status());
    }

    // create copyset exist
    {
        CopysetService_Stub stub(&channel_);
        brpc::Controller cntl;

        CreateCopysetRequest request;
        CreateCopysetResponse response;

        auto* copyset = request.add_copysets();
        copyset->set_poolid(poolId_);
        copyset->set_copysetid(copysetId_);
        copyset->add_peers()->set_address("127.0.0.1:29960:0");
        copyset->add_peers()->set_address("127.0.0.1:29961:0");
        copyset->add_peers()->set_address("127.0.0.1:29963:0");

        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST,
                  response.status());
    }
}

TEST_F(CopysetServiceTest, CreateCopysetTest_PeerParseFailed) {
    CopysetService_Stub stub(&channel_);
    brpc::Controller cntl;

    CreateCopysetRequest request;
    CreateCopysetResponse response;

    auto* copyset = request.add_copysets();
    copyset->set_poolid(poolId_);
    copyset->set_copysetid(copysetId_);
    copyset->add_peers()->set_address("127.0.0.1:abcd:0");
    copyset->add_peers()->set_address("127.0.0.1:efgh:0");
    copyset->add_peers()->set_address("127.0.0.1:ijkl:0");

    stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_PARSE_PEER_ERROR,
                response.status());
}

TEST_F(CopysetServiceTest, GetCopyestStauts_CopysetNodeNotExists) {
    CopysetService_Stub stub(&channel_);
    brpc::Controller cntl;

    CopysetStatusRequest request;
    CopysetStatusResponse response;

    request.set_poolid(poolId_);
    request.set_copysetid(copysetId_);
    request.mutable_peer()->set_address("127.0.0.1:29960:0");

    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST,
                response.status());
}

TEST_F(CopysetServiceTest, GetCopyestStauts_PeerMismatch) {
    // create one copyset
    {
        CopysetService_Stub stub(&channel_);
        brpc::Controller cntl;

        CreateCopysetRequest request;
        CreateCopysetResponse response;

        auto* copyset = request.add_copysets();
        copyset->set_poolid(poolId_);
        copyset->set_copysetid(copysetId_);
        copyset->add_peers()->set_address("127.0.0.1:29960:0");
        copyset->add_peers()->set_address("127.0.0.1:29961:0");
        copyset->add_peers()->set_address("127.0.0.1:29962:0");

        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS,
                  response.status());
    }

    CopysetService_Stub stub(&channel_);
    brpc::Controller cntl;

    CopysetStatusRequest request;
    CopysetStatusResponse response;

    request.set_poolid(poolId_);
    request.set_copysetid(copysetId_);
    request.mutable_peer()->set_address("127.0.0.1:29961:0");

    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_PEER_MISMATCH,
              response.status());
}

TEST_F(CopysetServiceTest, GetCopyestStauts) {
    // create one copyset
    {
        CopysetService_Stub stub(&channel_);
        brpc::Controller cntl;

        CreateCopysetRequest request;
        CreateCopysetResponse response;

        auto* copyset = request.add_copysets();
        copyset->set_poolid(poolId_);
        copyset->set_copysetid(copysetId_);
        copyset->add_peers()->set_address("127.0.0.1:29960:0");
        copyset->add_peers()->set_address("127.0.0.1:29961:0");
        copyset->add_peers()->set_address("127.0.0.1:29962:0");

        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS,
                  response.status());
    }

    CopysetService_Stub stub(&channel_);
    brpc::Controller cntl;

    CopysetStatusRequest request;
    CopysetStatusResponse response;

    request.set_poolid(poolId_);
    request.set_copysetid(copysetId_);
    request.mutable_peer()->set_address("127.0.0.1:29960:0");

    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS, response.status());
    LOG(INFO) << response.ShortDebugString();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
