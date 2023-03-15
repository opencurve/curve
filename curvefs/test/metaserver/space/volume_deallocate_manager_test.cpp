/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Tue Apr 25 19:35:13 CST 2023
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include "curvefs/test/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/space/volume_deallocate_manager.h"
#include "curvefs/test/metaserver/space/mock_volume_space_manager.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "curvefs/test/metaserver/space/utils.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"


namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::MockMetaServerClient;
using curvefs::metaserver::storage::RandomStoragePath;
using curvefs::metaserver::storage::RocksDBStorage;
using curvefs::metaserver::storage::StorageOptions;

using ::testing::_;
using ::testing::Return;

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

TEST(VolumeDeallocateManager, Run) {
    auto &instance = VolumeDeallocateManager::GetInstance();
    VolumeDeallocateWorkerQueueOption workerOpt;
    workerOpt.enable = true;
    workerOpt.workerNum = 10;

    VolumeDeallocateExecuteOption executeOpt;
    auto volumeSpaceManager = std::make_shared<MockVolumeSpaceManager>();
    auto metaClient = std::make_shared<MockMetaServerClient>();
    auto copyset = std::make_shared<copyset::MockCopysetNode>();
    executeOpt.volumeSpaceManager = volumeSpaceManager;
    executeOpt.metaClient = metaClient;
    executeOpt.batchClean = 10;

    // double run
    {
        instance.Init(workerOpt, executeOpt);
        instance.Run();
        instance.Run();
        instance.Stop();
    }

    // register and run
    {
        instance.Run();

        // prepare task
        auto dataDir = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir;
        options.localFileSystem = localfs.get();
        auto kvStorage = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage->Open());

        uint64_t fsId = 1;
        uint64_t blockGroupSize = 128 * 1024 * 1024;

        EXPECT_CALL(*volumeSpaceManager, GetBlockGroupSize(_))
            .WillRepeatedly(Return(blockGroupSize));
        EXPECT_CALL(*copyset, IsLeaderTerm())
            .WillOnce(Return(false))
            .WillRepeatedly(Return(true));

        std::thread t([&]() {
            for (int i = 1; i <= 100; i++) {
                auto nameGen = std::make_shared<NameGenerator>(i);
                auto inodeStorage =
                    std::make_shared<InodeStorage>(kvStorage, nameGen, 0);

                VolumeDeallocateCalOption calOpt;
                calOpt.kvStorage = kvStorage;
                calOpt.inodeStorage = inodeStorage;
                calOpt.nameGen = nameGen;

                InodeVolumeSpaceDeallocate task(fsId, i, copyset);
                task.Init(calOpt);

                instance.Register(task);

                if (i >= 2 && i <= 20) {
                    instance.Cancel(i - 1);
                }

                if (i > 20 && i < 40) {
                    instance.HasDeallocate();
                    instance.Deallocate(i - 1, 0);
                }
            }
        });

        // wait and stop
        sleep(1);
        t.join();
        ASSERT_FALSE(instance.HasDeallocate());
        instance.Stop();

        ASSERT_TRUE(kvStorage->Close());
        auto output = execShell("rm -rf " + dataDir);
        ASSERT_EQ(output.size(), 0);
    }
}

}  // namespace metaserver
}  // namespace curvefs
