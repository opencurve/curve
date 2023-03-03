/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Wednesday Jul 13 16:51:31 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/s3compact_worker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/metaserver/s3compact.h"
#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/iterator.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"
#include "curvefs/test/metaserver/mock/mock_kv_storage.h"

namespace curvefs {
namespace metaserver {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class S3CompactWorkerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        manager_ = &S3CompactManager::GetInstance();

        workerContext_.running = true;
        workerOptions_.sleepMS = 100;
    }

 protected:
    S3CompactManager* manager_;
    S3CompactWorkerContext workerContext_;
    S3CompactWorkerOptions workerOptions_;
    copyset::MockCopysetNodeManager copysetNodeManager_;
};

TEST_F(S3CompactWorkerTest, TestRunAndStop) {
    S3CompactWorker worker(manager_, &workerContext_, &workerOptions_);
    ASSERT_NO_FATAL_FAILURE(worker.Run());

    workerContext_.running = false;
    workerContext_.cond.notify_all();
    ASSERT_NO_FATAL_FAILURE(worker.Stop());
}

namespace {

std::shared_ptr<std::map<std::string, std::string>> PrepareInodeList() {
    const uint32_t fsId = 1;
    const uint64_t inodes = 10ULL * 10000;
    std::map<std::string, std::string> kvs;

    for (uint64_t i = 0; i < inodes; ++i) {
        storage::Key4Inode key(fsId, i);
        kvs.emplace(key.SerializeToString(), "dummy");
    }

    return std::make_shared<std::map<std::string, std::string>>(std::move(kvs));
}

}  // namespace

TEST_F(S3CompactWorkerTest, TestCancelCompact) {
    auto mockKvStorage = std::make_shared<storage::MockKVStorage>();
    auto nameGen = std::make_shared<NameGenerator>(1);
    auto inodeStorage =
        std::make_shared<InodeStorage>(mockKvStorage, nameGen, 0);
    FileType2InodeNumMap filetype2InodeNum;
    auto inodeManager = std::make_shared<InodeManager>(inodeStorage, nullptr,
                                                       &filetype2InodeNum);
    auto copysetNode = std::make_shared<copyset::MockCopysetNode>();
    auto kvs = PrepareInodeList();

    unsigned int seed = time(nullptr);

    S3CompactWorker worker(manager_, &workerContext_,
                           &workerOptions_);
    ASSERT_NO_FATAL_FAILURE(worker.Run());

    EXPECT_CALL(*mockKvStorage, HGetAll(_))
        .WillRepeatedly(Invoke([&kvs](const std::string& /*name*/) {
            return std::make_shared<
                storage::ContainerIterator<std::map<std::string, std::string>>>(
                kvs);
        }));

    EXPECT_CALL(*mockKvStorage, HGet(_, _, _))
        .WillRepeatedly(Return(storage::Status::NotFound()));

    EXPECT_CALL(*copysetNode, IsLeaderTerm())
        .WillRepeatedly(Invoke([&]() {
            return rand_r(&seed) % 2 == 0;
        }));

    // generate a fake compaction job
    PartitionInfo pinfo;
    pinfo.set_poolid(1);
    pinfo.set_copysetid(1);
    pinfo.set_partitionid(1);
    pinfo.set_fsid(1);

    S3Compact compact(inodeManager, copysetNode, pinfo);

    {
        std::lock_guard<std::mutex> lock(workerContext_.mtx);
        workerContext_.s3compacts.push_back(compact);
    }
    workerContext_.cond.notify_one();

    std::this_thread::sleep_for(
        std::chrono::seconds(rand_r(&seed) % 10 + 5));

    worker.Cancel(pinfo.partitionid());

    workerContext_.running = false;
    workerContext_.cond.notify_all();
    ASSERT_NO_FATAL_FAILURE(worker.Stop());
}

}  // namespace metaserver
}  // namespace curvefs
