/*
 * Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: Curve
 * Created Date: 2021-09-02
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <memory>
#include <unordered_map>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/process.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/src/metaserver/storage/storage_fstream.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ContainerType = std::unordered_map<std::string, std::string>;

class StorageFstreamTest : public ::testing::Test {
 protected:
    StorageFstreamTest()
    : dirname_(".dump"),
      pathname_(".dump/storage.dump") {}

    void SetUp() override {
        std::string ret;
        ASSERT_TRUE(ExecShell("mkdir -p " + dirname_, &ret));
    }

    void TearDown() override {
        std::string ret;
        ASSERT_TRUE(ExecShell("rm -rf " + dirname_, &ret));
    }

    bool ExecShell(const std::string& cmd, std::string* ret) {
        std::array<char, 128> buffer;
        std::unique_ptr<FILE, decltype(&pclose)>
            pipe(popen(cmd.c_str(), "r"), pclose);
        if (!pipe) {
            return false;
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            *ret += buffer.data();
        }
        return true;
    }

    void TestSave(std::shared_ptr<KVStorage> kvStorage, bool background);

 protected:
    std::string dirname_;
    std::string pathname_;
};

void StorageFstreamTest::TestSave(std::shared_ptr<KVStorage> kvStorage,
                                  bool background) {
    // step1: prepare data
    auto container = std::make_shared<ContainerType>();
    container->emplace("k1", "v1");
    auto patitionIterator = std::make_shared<ContainerIterator<ContainerType>>(
        container);

    Status s;
    s = kvStorage->HSet("partition:2", "k2", "v2");
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet("partition:3", "k3", "v3");
    ASSERT_TRUE(s.ok());

    // step2: save to file
    auto children = std::vector<std::shared_ptr<Iterator>>{
        std::make_shared<IteratorWrapper>(
            ENTRY_TYPE::PARTITION, 1, patitionIterator),
        std::make_shared<IteratorWrapper>(
            ENTRY_TYPE::INODE, 2, kvStorage->HGetAll("partition:2")),
        std::make_shared<IteratorWrapper>(
            ENTRY_TYPE::DENTRY, 3, kvStorage->SGetAll("partition:3")),
    };
    auto iterator = std::make_shared<MergeIterator>(children);
    ASSERT_TRUE(SaveToFile(pathname_, iterator, false));

    // step3: load from file
    size_t nPartition = 0;
    size_t nInode = 0;
    size_t nDentry = 0;
    auto callback = [&](ENTRY_TYPE type,
                        uint32_t partitionId,
                        const std::string& key,
                        const std::string& value) {
        if (type == ENTRY_TYPE::PARTITION) {
            if (partitionId == 1 && key == "k1" && value == "v1") {
                nPartition++;
            }
        } else if (type == ENTRY_TYPE::INODE) {
            if (partitionId == 2 && key == "k2" && value == "v2") {
                nInode++;
            }
        } else if (type == ENTRY_TYPE::DENTRY) {
            if (partitionId == 3 && key == "k3" && value == "v3") {
                nDentry++;
            }
        }
        return true;
    };
    ASSERT_TRUE(LoadFromFile<decltype(callback)>(pathname_, callback));
    ASSERT_EQ(nPartition, 1);
    ASSERT_EQ(nInode, 1);
    ASSERT_EQ(nDentry, 1);
}

TEST_F(StorageFstreamTest, MemoryStorageSave) {
    StorageOptions options;
    auto kvStorage = std::make_shared<MemoryStorage>(options);
    TestSave(kvStorage, false);
}

TEST_F(StorageFstreamTest, MemoryStorageSaveBackground) {
    StorageOptions options;
    auto kvStorage = std::make_shared<MemoryStorage>(options);
    TestSave(kvStorage, true);
}

TEST_F(StorageFstreamTest, RocksDBStorageSave) {
    StorageOptions options;
    // auto kvStorage = std::make_shared<RocksDBStorage>(options);
    auto kvStorage = std::make_shared<MemoryStorage>(options);
    TestSave(kvStorage, false);
}

TEST_F(StorageFstreamTest, RocksDBStorageSaveBackground) {
    StorageOptions options;
    // auto kvStorage = std::make_shared<RocksDBStorage>(options);
    auto kvStorage = std::make_shared<MemoryStorage>(options);
    TestSave(kvStorage, true);
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
