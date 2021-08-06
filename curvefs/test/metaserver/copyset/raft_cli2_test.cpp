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
 * Date: Mon Sep  6 19:01:35 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/raft_cli2.h"

#include <brpc/server.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "curvefs/test/metaserver/copyset/mock/mock_raft_cli2_service.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::testing::_;
using ::testing::Invoke;

struct FakeRpcService {
    explicit FakeRpcService(int cntlErrCode) : cntlErrCode(cntlErrCode) {}
    explicit FakeRpcService(const std::string& leaderIdStr)
        : leaderIdStr(leaderIdStr) {}

    template <typename RequestT, typename ResponseT>
    void operator()(google::protobuf::RpcController* cntl,
                    const RequestT* request, ResponseT* response,
                    google::protobuf::Closure* done) {
        if (cntlErrCode != 0) {
            static_cast<brpc::Controller*>(cntl)->SetFailed(cntlErrCode, "%d",
                                                            cntlErrCode);
        } else {
            response->mutable_leader()->set_address(leaderIdStr);
        }
        done->Run();
    }

    int cntlErrCode = 0;
    std::string leaderIdStr;
};

class RaftCli2Test : public testing::Test {
 protected:
    static void SetUpTestCase() {
        LOG(INFO) << "RaftCli2Test SetUpTestCase";

        for (auto port : {29901, 29902, 29903}) {
            auto service = absl::make_unique<MockRaftCli2Service>();
            auto server = absl::make_unique<brpc::Server>();

            ASSERT_EQ(0, server->AddService(service.get(),
                                            brpc::SERVER_DOESNT_OWN_SERVICE));
            butil::EndPoint listenAddr(butil::IP_ANY, port);
            ASSERT_EQ(0, server->Start(listenAddr, nullptr));

            services_.emplace_back(std::move(server), std::move(service));

            conf_.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, port}});
        }
    }

    static void TearDownTestCase() {
        for (auto& s : services_) {
            s.first->Stop(0);
            s.first->Join();
        }

        LOG(INFO) << "RaftCli2Test TearDownTestCase";
    }

 protected:
    static std::vector<std::pair<std::unique_ptr<brpc::Server>,
                                 std::unique_ptr<MockRaftCli2Service>>>
        services_;

    static braft::Configuration conf_;
    braft::PeerId leaderId_;
    PoolId poolId_ = 1;
    CopysetId copysetId_ = 1;
};

std::vector<std::pair<std::unique_ptr<brpc::Server>,
                      std::unique_ptr<MockRaftCli2Service>>>
    RaftCli2Test::services_;
braft::Configuration RaftCli2Test::conf_;

TEST_F(RaftCli2Test, ConfIsEmpty) {
    braft::Configuration conf;
    auto status = GetLeader(poolId_, copysetId_, conf, &leaderId_);
    EXPECT_EQ(EINVAL, status.error_code());
}

TEST_F(RaftCli2Test, ConfAddressIsInvalid) {
    braft::Configuration conf;
    conf.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, 123456}});
    conf.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, 123457}});
    conf.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, 123458}});

    auto status = GetLeader(poolId_, copysetId_, conf, &leaderId_);
    EXPECT_EQ(-1, status.error_code());
}

TEST_F(RaftCli2Test, AllServersAreNotAvailable) {
    braft::Configuration conf;
    conf.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, 29905}});
    conf.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, 29906}});
    conf.add_peer(braft::PeerId{butil::EndPoint{butil::IP_ANY, 29907}});

    auto status = GetLeader(poolId_, copysetId_, conf, &leaderId_);
    EXPECT_EQ(112, status.error_code());
}

TEST_F(RaftCli2Test, AllServersReturnEINVAL) {
    EXPECT_CALL(*services_[0].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));
    EXPECT_CALL(*services_[1].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));
    EXPECT_CALL(*services_[2].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));

    auto status = GetLeader(poolId_, copysetId_, conf_, &leaderId_);
    EXPECT_EQ(EINVAL, status.error_code());
}

TEST_F(RaftCli2Test, GetLeaderLoopThroughAllPeersUntilSuccess) {
    std::string leaderAddress = "127.0.0.1:29904:0";

    EXPECT_CALL(*services_[0].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));
    EXPECT_CALL(*services_[1].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));
    EXPECT_CALL(*services_[2].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{leaderAddress}));

    auto status = GetLeader(poolId_, copysetId_, conf_, &leaderId_);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(leaderAddress, leaderId_.to_string());
}

TEST_F(RaftCli2Test, AllServersReturnEmptyLeaderId) {
    std::string leaderAddress;

    EXPECT_CALL(*services_[0].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));
    EXPECT_CALL(*services_[1].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{EINVAL}));
    EXPECT_CALL(*services_[2].second.get(), GetLeader(_, _, _, _))
        .WillOnce(Invoke(FakeRpcService{leaderAddress}));

    auto status = GetLeader(poolId_, copysetId_, conf_, &leaderId_);
    EXPECT_EQ(EINVAL, status.error_code());
    EXPECT_TRUE(leaderId_.is_empty());
}


}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
