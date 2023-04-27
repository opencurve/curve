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
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/storage/storage_fstream.h"
#include "curvefs/test/metaserver/storage/storage_test.h"
#include "src/fs/ext4_filesystem_impl.h"

namespace curvefs {
namespace metaserver {
namespace storage {

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

using ContainerType = std::unordered_map<std::string, std::string>;

class StorageFstreamTest : public ::testing::Test {
 protected:
    StorageFstreamTest()
    : dirname_(".dump"),
      pathname_(".dump/storage.dump"),
      dbpath_(".dump/rocksdb") {}

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

    std::string SerializeToString(const Dentry& dentry) {
        std::string value;
        bool succ = dentry.SerializeToString(&value);
        return succ ? value : "";
    }

    std::string TableName(uint32_t partitionId) {
        auto ng = std::make_shared<NameGenerator>(partitionId);
        return ng->GetDentryTableName();
    }

 protected:
    std::string dirname_;
    std::string pathname_;
    std::string dbpath_;
    Converter conv_;
};

void StorageFstreamTest::TestSave(std::shared_ptr<KVStorage> kvStorage,
                                  bool background) {
    // step1: prepare data
    auto container = std::make_shared<ContainerType>();
    container->emplace("k1", "v1");
    auto patitionIterator = std::make_shared<ContainerIterator<ContainerType>>(
        container);

    Status s;
    s = kvStorage->HSet(TableName(2), "k2", Value("v2"));
    ASSERT_TRUE(s.ok());
    s = kvStorage->SSet(TableName(3), "k3", Value("v3"));
    ASSERT_TRUE(s.ok());

    // step2: save to file
    auto children = std::vector<std::shared_ptr<Iterator>>{
        std::make_shared<IteratorWrapper>(
            ENTRY_TYPE::PARTITION, 1, patitionIterator),
        std::make_shared<IteratorWrapper>(
            ENTRY_TYPE::INODE, 2, kvStorage->HGetAll(TableName(2))),
        std::make_shared<IteratorWrapper>(
            ENTRY_TYPE::DENTRY, 3, kvStorage->SGetAll(TableName(3))),
    };
    auto iterator = std::make_shared<MergeIterator>(children);
    ASSERT_TRUE(SaveToFile(pathname_, iterator, background));

    // step3: load from file
    size_t nPartition = 0;
    size_t nInode = 0;
    size_t nDentry = 0;
    uint8_t nVersion = 0;
    std::string str4Inode, str4Dentry;
    ASSERT_TRUE(conv_.SerializeToString(Value("v2"), &str4Inode));
    ASSERT_TRUE(conv_.SerializeToString(Value("v3"), &str4Dentry));
    auto callback = [&](uint8_t version,
                        ENTRY_TYPE type,
                        uint32_t partitionId,
                        absl::string_view key,
                        absl::string_view value) {
        if (version == 2) {
            nVersion++;
        }

        if (type == ENTRY_TYPE::PARTITION) {
            if (partitionId == 1 && key == "k1" && value == "v1") {
                nPartition++;
            }
        } else if (type == ENTRY_TYPE::INODE) {
            if (partitionId == 2 && key == "k2" && value == str4Inode) {
                nInode++;
            }
        } else if (type == ENTRY_TYPE::DENTRY) {
            if (partitionId == 3 && key == "k3" && value == str4Dentry) {
                nDentry++;
            }
        }
        return true;
    };

    uint8_t dummyVerion;
    ASSERT_TRUE(LoadFromFile(pathname_, &dummyVerion, callback));
    ASSERT_EQ(nPartition, 1);
    ASSERT_EQ(nInode, 1);
    ASSERT_EQ(nDentry, 1);
}

TEST_F(StorageFstreamTest, DISABLED_MemoryStorageSave) {
    StorageOptions options;
    auto kvStorage = std::make_shared<MemoryStorage>(options);
    TestSave(kvStorage, false);
}

TEST_F(StorageFstreamTest, DISABLED_MemoryStorageSaveBackground) {
    StorageOptions options;
    auto kvStorage = std::make_shared<MemoryStorage>(options);
    TestSave(kvStorage, true);
}

TEST_F(StorageFstreamTest, RocksDBStorageSave) {
    StorageOptions options;
    options.dataDir = dbpath_;
    options.localFileSystem = localfs.get();
    auto kvStorage = std::make_shared<RocksDBStorage>(options);
    ASSERT_TRUE(kvStorage->Open());
    TestSave(kvStorage, false);
    ASSERT_TRUE(kvStorage->Close());
}

TEST_F(StorageFstreamTest, MiscTest) {
    // CASE 1: convert type to string
    ASSERT_EQ(Type2Str(ENTRY_TYPE::INODE), "i");
    ASSERT_EQ(Type2Str(ENTRY_TYPE::DENTRY), "d");
    ASSERT_EQ(Type2Str(ENTRY_TYPE::PARTITION), "p");
    ASSERT_EQ(Type2Str(ENTRY_TYPE::PENDING_TX), "t");
    ASSERT_EQ(Type2Str(ENTRY_TYPE::S3_CHUNK_INFO_LIST), "s");
    ASSERT_EQ(Type2Str(ENTRY_TYPE::VOLUME_EXTENT), "v");
    ASSERT_EQ(Type2Str(ENTRY_TYPE::UNKNOWN), "u");

    ASSERT_EQ(Str2Type("i"), ENTRY_TYPE::INODE);
    ASSERT_EQ(Str2Type("d"), ENTRY_TYPE::DENTRY);
    ASSERT_EQ(Str2Type("p"), ENTRY_TYPE::PARTITION);
    ASSERT_EQ(Str2Type("t"), ENTRY_TYPE::PENDING_TX);
    ASSERT_EQ(Str2Type("s"), ENTRY_TYPE::S3_CHUNK_INFO_LIST);
    ASSERT_EQ(Str2Type("v"), ENTRY_TYPE::VOLUME_EXTENT);
    ASSERT_EQ(Str2Type("u"), ENTRY_TYPE::UNKNOWN);
    ASSERT_EQ(Str2Type("x"), ENTRY_TYPE::UNKNOWN);
    ASSERT_EQ(Str2Type("y"), ENTRY_TYPE::UNKNOWN);

    {
        auto ret = Extract("d100");
        ASSERT_EQ(ret.first, ENTRY_TYPE::DENTRY);
        ASSERT_EQ(ret.second, 100);

        ret = Extract("i0");
        ASSERT_EQ(ret.first, ENTRY_TYPE::INODE);
        ASSERT_EQ(ret.second, 0);

        ret = Extract("");
        ASSERT_EQ(ret.first, ENTRY_TYPE::UNKNOWN);
        ASSERT_EQ(ret.second, 0);
    }

    {
        auto ret = UserKey("t3:");
        ASSERT_EQ(ret.first, "t3");
        ASSERT_EQ(ret.second, "");

        ret = UserKey("i100:000");
        ASSERT_EQ(ret.first, "i100");
        ASSERT_EQ(ret.second, "000");

        ret = UserKey("");
        ASSERT_EQ(ret.first, "");
        ASSERT_EQ(ret.second, "");
    }

    // CASE 2: open file failed when save
    ASSERT_FALSE(SaveToFile("/__not_found__/dumpfile", nullptr, false));

    // CASE 3: open file failed when load
    uint8_t dummyVersion;
    {
        auto callback = [&](uint8_t version,
                            ENTRY_TYPE type,
                            uint32_t partitionId,
                            absl::string_view key,
                            absl::string_view value) {
            return true;
        };
        bool succ = LoadFromFile("__not_found__", &dummyVersion, callback);
        ASSERT_FALSE(succ);
    }

    // CASE 4: invoke failed
    {
        auto callback = [&](uint8_t version,
                            ENTRY_TYPE type,
                            uint32_t partitionId,
                            absl::string_view key,
                            absl::string_view value) {
            return false;
        };
        bool succ = LoadFromFile(pathname_, &dummyVersion, callback);
        ASSERT_FALSE(succ);
    }
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
