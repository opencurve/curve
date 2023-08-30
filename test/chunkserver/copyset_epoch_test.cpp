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
 * Created Date: 18-10-22
 * Author: wudemiao
 */


#include <unistd.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "test/chunkserver/chunkserver_test_util.h"
#include "src/common/uuid.h"
#include "src/chunkserver/chunk_service.h"
#include "src/common/concurrent/concurrent.h"
#include "src/fs/fs_common.h"

#define BRAFT_SNAPSHOT_PATTERN "snapshot_%020" PRId64

namespace curve {
namespace chunkserver {

using curve::common::Thread;
using curve::common::UUIDGenerator;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

class CopysetEpochTest : public testing::Test {
 protected:
    virtual void SetUp() {
        UUIDGenerator uuidGenerator;
        dir1 = uuidGenerator.GenerateUUID();
        Exec(("mkdir " + dir1).c_str());
    }
    virtual void TearDown() {
        Exec(("rm -fr " + dir1).c_str());
    }

 public:
    std::string dir1;
};

butil::AtExitManager atExitManager;

TEST_F(CopysetEpochTest, DISABLED_basic) {
    const char *ip = "127.0.0.1";
    int port = 9026;
    const char *confs = "127.0.0.1:9026:0";
    int snapshotInterval = 1;
    int electionTimeoutMs = 3000;
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    std::string snapshotPath = dir1 + "/4295067297/raft_snapshot";
    uint64_t lastIncludeIndex = 0;

    /**
     *Start a chunkserver
     */
    std::string copysetdir = "local://./" + dir1;
    auto startChunkServerFunc = [&] {
        StartChunkserver(ip,
                         port + 0,
                         copysetdir.c_str(),
                         confs,
                         snapshotInterval,
                         electionTimeoutMs);
    };

    Thread t1(startChunkServerFunc);
    t1.detach();

    PeerId leader;
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    Configuration conf;
    conf.parse_from(confs);

    ::usleep(1000 * electionTimeoutMs);

    {
        butil::Status status = WaitLeader(logicPoolId,
                                          copysetId,
                                          conf,
                                          &leader,
                                          electionTimeoutMs);
        LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
        ASSERT_TRUE(status.ok());
    }

    CopysetNodeManager &nodeManager = CopysetNodeManager::GetInstance();
    auto node = nodeManager.GetCopysetNode(logicPoolId, copysetId);
    ASSERT_EQ(1, node->GetConfEpoch());
    std::string confEpochPath1 = snapshotPath;
    butil::string_appendf(&confEpochPath1,
                          "/" BRAFT_SNAPSHOT_PATTERN,
                          ++lastIncludeIndex);
    confEpochPath1.append("/");
    confEpochPath1.append(kCurveConfEpochFilename);
    ASSERT_EQ(true, fs->FileExists(confEpochPath1));

    //Waiting for snapshot generation
    ::sleep(2 * snapshotInterval);

    //When the node is shut down and restarted, a load snapshot will be executed to load the epoch from the snapshot
    node->Fini();
    node->Run();
    {
        butil::Status status = WaitLeader(logicPoolId,
                                          copysetId,
                                          conf,
                                          &leader,
                                          electionTimeoutMs);
        LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
        ASSERT_TRUE(status.ok());
    }
    ASSERT_EQ(2, node->GetConfEpoch());
    std::string confEpochPath2 = snapshotPath;
    butil::string_appendf(&confEpochPath2,
                          "/" BRAFT_SNAPSHOT_PATTERN,
                          ++lastIncludeIndex);
    confEpochPath2.append("/");
    confEpochPath2.append(kCurveConfEpochFilename);
    ASSERT_EQ(true, fs->FileExists(confEpochPath2));

    //Waiting for snapshot generation
    ::sleep(2 * snapshotInterval);

    //When the node is shut down and restarted, a load snapshot will be executed to load the epoch from the snapshot
    node->Fini();
    node->Run();
    {
        butil::Status status = WaitLeader(logicPoolId,
                                          copysetId,
                                          conf,
                                          &leader,
                                          electionTimeoutMs);
        LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
        ASSERT_TRUE(status.ok());
    }
    ASSERT_EQ(3, node->GetConfEpoch());
    std::string confEpochPath3 = snapshotPath;
    butil::string_appendf(&confEpochPath3,
                          "/" BRAFT_SNAPSHOT_PATTERN,
                          ++lastIncludeIndex);
    confEpochPath3.append("/");
    confEpochPath3.append(kCurveConfEpochFilename);
    ASSERT_EQ(true, fs->FileExists(confEpochPath3));

    //When the node is shut down and restarted, a load snapshot will be executed to load the epoch from the snapshot
    node->Fini();
    node->Run();
    {
        butil::Status status = WaitLeader(logicPoolId,
                                          copysetId,
                                          conf,
                                          &leader,
                                          electionTimeoutMs);
        LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
        ASSERT_TRUE(status.ok());
    }
    ASSERT_EQ(4, node->GetConfEpoch());
    std::string confEpochPath4 = snapshotPath;
    butil::string_appendf(&confEpochPath4,
                          "/" BRAFT_SNAPSHOT_PATTERN,
                          ++lastIncludeIndex);
    confEpochPath4.append("/");
    confEpochPath4.append(kCurveConfEpochFilename);
    ASSERT_EQ(true, fs->FileExists(confEpochPath4));

    //When the node is shut down and restarted, a load snapshot will be executed to load the epoch from the snapshot
    node->Fini();
    node->Run();
    {
        butil::Status status = WaitLeader(logicPoolId,
                                          copysetId,
                                          conf,
                                          &leader,
                                          electionTimeoutMs);
        LOG_IF(INFO, status.ok()) << "leader id: " << leader.to_string();
        ASSERT_TRUE(status.ok());
    }
    ASSERT_EQ(5, node->GetConfEpoch());
    std::string confEpochPath5 = snapshotPath;
    butil::string_appendf(&confEpochPath5,
                          "/" BRAFT_SNAPSHOT_PATTERN,
                          ++lastIncludeIndex);
    confEpochPath5.append("/");
    confEpochPath5.append(kCurveConfEpochFilename);
    ASSERT_EQ(true, fs->FileExists(confEpochPath5));

    node->Fini();
}

}  // namespace chunkserver
}  // namespace curve
