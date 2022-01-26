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
 * Date: Sat Sep  4 14:08:04 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/meta_operator.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <mutex>
#include <regex>

#include "absl/memory/memory.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"
#include "curvefs/test/metaserver/copyset/mock/mock_raft_node.h"
#include "curvefs/test/metaserver/mock/mock_metastore.h"
#include "curvefs/test/utils/protobuf_message_utils.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

const int kDummyServerPort = 32000;

template <typename RequestT, typename ResponseT,
          MetaStatusCode code = MetaStatusCode::UNKNOWN_ERROR>
MetaStatusCode FakeOnApplyFunc(const RequestT* request, ResponseT* response) {
    response->set_statuscode(code);
    return code;
}

class FakeClosure : public google::protobuf::Closure {
 public:
    void Run() override {
        std::lock_guard<std::mutex> lk(mtx_);
        runned_ = true;
        cond_.notify_one();
    }

    void WaitRunned() {
        std::unique_lock<std::mutex> lk(mtx_);
        cond_.wait(lk, [this]() { return runned_; });
    }

    bool Runned() const {
        return runned_;
    }

 private:
    std::mutex mtx_;
    std::condition_variable cond_;
    bool runned_ = false;
};

std::string Exec(const std::string& cmd) {
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        return "ERROR";
    }
    char buffer[4096];
    std::string result;
    while (!feof(pipe)) {
        if (fgets(buffer, 1024, pipe) != NULL) result += buffer;
    }
    pclose(pipe);
    return result;
}

using ::curve::common::TimeUtility;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;

class MetaOperatorTest : public testing::Test {
 protected:
    static void SetUpTestCase() {
        ASSERT_EQ(0, brpc::StartDummyServerAt(kDummyServerPort));
    }

    static bool CheckMetric(const std::string& curlCmd, uint64_t expectValue) {
        std::string cmdResult = Exec(curlCmd);
        if (cmdResult.empty() || cmdResult == "ERROR") {
            return false;
        }

        LOG(INFO) << cmdResult;
        std::regex pattern("(.*?) : (\\d+)");
        std::smatch match;

        if (!std::regex_search(cmdResult, match, pattern)) {
            return false;
        }

        return std::stoull(match[2].str()) == expectValue;
    }

 protected:
    MockCopysetNodeManager mockNodeManager_;
};

TEST_F(MetaOperatorTest, OperatorTypeTest) {
    std::unique_ptr<MetaOperator> meta;

#define TEST_OPERATOR_TYPE(TYPE)                                    \
    do {                                                            \
        meta = absl::make_unique<TYPE##Operator>(nullptr, nullptr); \
        EXPECT_EQ(OperatorType::TYPE, meta->GetOperatorType());     \
    } while (0)

    TEST_OPERATOR_TYPE(GetDentry);
    TEST_OPERATOR_TYPE(ListDentry);
    TEST_OPERATOR_TYPE(CreateDentry);
    TEST_OPERATOR_TYPE(DeleteDentry);
    TEST_OPERATOR_TYPE(GetInode);
    TEST_OPERATOR_TYPE(BatchGetInodeAttr);
    TEST_OPERATOR_TYPE(BatchGetXAttr);
    TEST_OPERATOR_TYPE(CreateInode);
    TEST_OPERATOR_TYPE(UpdateInode);
    TEST_OPERATOR_TYPE(GetOrModifyS3ChunkInfo);
    TEST_OPERATOR_TYPE(DeleteInode);
    TEST_OPERATOR_TYPE(CreateRootInode);
    TEST_OPERATOR_TYPE(CreatePartition);
    TEST_OPERATOR_TYPE(DeletePartition);
    TEST_OPERATOR_TYPE(PrepareRenameTx);

#undef TEST_OPERATOR_TYPE
}

TEST_F(MetaOperatorTest, OnApplyErrorTest) {
    PoolId poolId = 100;
    CopysetId copysetId = 100;
    braft::Configuration conf;

    CopysetNode node(poolId, copysetId, conf, &mockNodeManager_);
    mock::MockMetaStore* mockMetaStore = new mock::MockMetaStore();
    node.SetMetaStore(mockMetaStore);

    ON_CALL(*mockMetaStore, Clear())
        .WillByDefault(Return(true));

    brpc::Controller cntl;

#define OPERATOR_ON_APPLY_TEST(TYPE)                                           \
    {                                                                          \
        EXPECT_CALL(*mockMetaStore, TYPE(_, _))                                \
            .WillOnce(Invoke(FakeOnApplyFunc<TYPE##Request, TYPE##Response>)); \
        TYPE##Request request;                                                 \
        TYPE##Response response;                                               \
        FakeClosure closure;                                                   \
        auto op = absl::make_unique<TYPE##Operator>(&node, &cntl, &request,    \
                                                    &response, nullptr);       \
        op->OnApply(1, &closure, TimeUtility::GetTimeofDayUs());               \
        closure.WaitRunned();                                                  \
        EXPECT_EQ(MetaStatusCode::UNKNOWN_ERROR, response.statuscode());       \
    }

    OPERATOR_ON_APPLY_TEST(GetDentry);
    OPERATOR_ON_APPLY_TEST(ListDentry);
    OPERATOR_ON_APPLY_TEST(CreateDentry);
    OPERATOR_ON_APPLY_TEST(DeleteDentry);
    OPERATOR_ON_APPLY_TEST(GetInode);
    OPERATOR_ON_APPLY_TEST(BatchGetInodeAttr);
    OPERATOR_ON_APPLY_TEST(BatchGetXAttr);
    OPERATOR_ON_APPLY_TEST(CreateInode);
    OPERATOR_ON_APPLY_TEST(UpdateInode);
    OPERATOR_ON_APPLY_TEST(GetOrModifyS3ChunkInfo);
    OPERATOR_ON_APPLY_TEST(DeleteInode);
    OPERATOR_ON_APPLY_TEST(CreateRootInode);
    OPERATOR_ON_APPLY_TEST(CreatePartition);
    OPERATOR_ON_APPLY_TEST(DeletePartition);
    OPERATOR_ON_APPLY_TEST(PrepareRenameTx);

#undef OPERATOR_ON_APPLY_TEST

    EXPECT_TRUE(
        CheckMetric("curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
                        "/vars | grep "
                        "op_apply_pool_100_copyset_100_get_dentry_total_error",
                    1));
    EXPECT_TRUE(
        CheckMetric("curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
                        "/vars | grep "
                        "op_apply_pool_100_copyset_100_list_dentry_total_error",
                    1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_dentry_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_delete_dentry_total_error",
        1));
    EXPECT_TRUE(
        CheckMetric("curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
                        "/vars | grep "
                        "op_apply_pool_100_copyset_100_get_inode_total_error",
                    1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_update_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_delete_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_root_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_partition_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_delete_partition_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_prepare_rename_tx_total_error",
        1));
}

TEST_F(MetaOperatorTest, OnApplyFromLogErrorTest) {
    PoolId poolId = 100;
    CopysetId copysetId = 100;
    braft::Configuration conf;

    CopysetNode node(poolId, copysetId, conf, &mockNodeManager_);
    mock::MockMetaStore* mockMetaStore = new mock::MockMetaStore();
    node.SetMetaStore(mockMetaStore);

    ON_CALL(*mockMetaStore, Clear())
        .WillByDefault(Return(true));

    brpc::Controller cntl;

#define OPERATOR_ON_APPLY_FROM_LOG_TEST(TYPE)                                \
    {                                                                        \
        EXPECT_CALL(*mockMetaStore, TYPE(_, _))                              \
            .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));                \
        TYPE##Request request;                                               \
        auto op = absl::make_unique<TYPE##Operator>(&node, &request, false); \
        op->OnApplyFromLog(TimeUtility::GetTimeofDayUs());                   \
        op.release();                                                        \
    }

    OPERATOR_ON_APPLY_FROM_LOG_TEST(CreateDentry);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(DeleteDentry);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(CreateInode);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(UpdateInode);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(GetOrModifyS3ChunkInfo);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(DeleteInode);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(CreateRootInode);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(CreatePartition);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(DeletePartition);
    OPERATOR_ON_APPLY_FROM_LOG_TEST(PrepareRenameTx);

#undef OPERATOR_ON_APPLY_FROM_LOG_TEST

#define OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST(TYPE)                     \
    {                                                                        \
        EXPECT_CALL(*mockMetaStore, TYPE(_, _)).Times(0);                    \
        TYPE##Request request;                                               \
        auto op = absl::make_unique<TYPE##Operator>(&node, &request, false); \
        op->OnApplyFromLog(TimeUtility::GetTimeofDayUs());                   \
        op.release();                                                        \
    }

    OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST(GetDentry);
    OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST(ListDentry);
    OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST(GetInode);
    OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST(BatchGetInodeAttr);
    OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST(BatchGetXAttr);


#undef OPERATOR_ON_APPLY_FROM_LOG_DO_NOTHING_TEST

    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_dentry_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_delete_dentry_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_update_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_delete_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_root_inode_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_create_partition_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_delete_partition_total_error",
        1));
    EXPECT_TRUE(CheckMetric(
        "curl -s 0.0.0.0:" + std::to_string(kDummyServerPort) +
            "/vars | grep "
            "op_apply_pool_100_copyset_100_prepare_rename_tx_total_error",
        1));
}

TEST_F(MetaOperatorTest, PropostTest_IsNotLeader) {
    PoolId poolId = 100;
    CopysetId copysetId = 100;
    braft::Configuration conf;

    butil::Status status;
    status.set_error(EINVAL, "invalid");
    CopysetNode node(poolId, copysetId, conf, &mockNodeManager_);
    node.on_leader_stop(status);

    CreateInodeRequest request;
    CreateInodeResponse response;
    auto op = absl::make_unique<CreateInodeOperator>(&node, nullptr, &request,
                                                     &response, nullptr);
    op->Propose();
    EXPECT_EQ(MetaStatusCode::REDIRECTED, response.statuscode());
    EXPECT_FALSE(response.has_appliedindex());
}

TEST_F(MetaOperatorTest, PropostTest_RequestCanBypassProcess) {
    PoolId poolId = 100;
    CopysetId copysetId = 100;
    braft::Configuration conf;

    CopysetNode node(poolId, copysetId, conf, &mockNodeManager_);
    CopysetNodeOptions options;
    options.dataUri = "local:///mnt/data";

    EXPECT_TRUE(node.Init(options));
    auto* mockMetaStore = new mock::MockMetaStore();
    node.SetMetaStore(mockMetaStore);
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    ON_CALL(*mockMetaStore, Clear())
        .WillByDefault(Return(true));
    EXPECT_CALL(*mockRaftNode, apply(_))
        .Times(0);
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);
    EXPECT_CALL(*mockMetaStore, GetDentry(_, _))
        .WillOnce(Return(MetaStatusCode::OK));

    node.on_leader_start(1);
    node.UpdateAppliedIndex(101);

    GetDentryRequest request;
    request.set_appliedindex(100);
    GetDentryResponse response;
    auto op = absl::make_unique<GetDentryOperator>(&node, nullptr, &request,
                                                   &response, nullptr);
    op->Propose();
    op.release();

    node.FlushApplyQueue();

    EXPECT_TRUE(response.has_appliedindex());
    EXPECT_EQ(101, response.appliedindex());

    node.Stop();
}

TEST_F(MetaOperatorTest, PropostTest_PropostTaskFailed) {
    PoolId poolId = 100;
    CopysetId copysetId = 100;
    braft::Configuration conf;

    CopysetNode node(poolId, copysetId, conf, &mockNodeManager_);

    node.on_leader_start(1);
    node.UpdateAppliedIndex(101);

    GetDentryRequest request;
    request.set_poolid(1);
    request.set_copysetid(1);
    request.set_partitionid(1);
    request.set_fsid(1);
    request.set_parentinodeid(1);
    request.set_txid(1);

    // NOTE: serialize will check whether message's size is bigger than INT_MAX
    auto usedSize = request.ByteSize();
    request.set_allocated_name(
        new std::string(static_cast<uint64_t>(INT_MAX) - usedSize + 1, 'x'));

    GetDentryResponse response;
    auto op = absl::make_unique<GetDentryOperator>(&node, nullptr, &request,
                                                   &response, nullptr);
    op->Propose();
    EXPECT_EQ(MetaStatusCode::UNKNOWN_ERROR, response.statuscode());
    EXPECT_FALSE(response.has_appliedindex());
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
