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
#include <google/protobuf/util/message_differencer.h>

#include <memory>
#include <unordered_map>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/process.h"
#include "curvefs/src/metaserver/storage.h"

namespace curvefs {
namespace metaserver {

using Hash = std::unordered_map<std::string, std::string>;
using DentryHash = std::unordered_map<std::string, Dentry>;
using InodeHash = std::unordered_map<std::string, Inode>;
using google::protobuf::util::MessageDifferencer;

class HashIterator : public Iterator {
 public:
    explicit HashIterator(Hash* hash)
        : hash_(hash) {}

    uint64_t Size() override { return hash_->size(); };
    bool Valid() override { return iter_ != hash_->end(); }
    void SeekToFirst() override { iter_ = hash_->begin(); }
    void Next() override { iter_++; }
    std::string Key() override { return iter_->first; }
    std::string Value() override { return iter_->second; }
    int Status() override { return 0; }

 private:
    Hash::iterator iter_;
    Hash* hash_;
};

class StorageTest : public ::testing::Test {
 protected:
    StorageTest()
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

    Dentry GenDentry() {
        Dentry dentry;
        dentry.set_fsid(1);
        dentry.set_parentinodeid(0);
        dentry.set_name("test");
        dentry.set_inodeid(1);
        dentry.set_txid(100);
        return dentry;
    }

    Inode GenInode() {
        Inode inode;
        inode.set_fsid(1);
        inode.set_inodeid(100);
        inode.set_length(4096);
        inode.set_ctime(0);
        inode.set_ctime_ns(0);
        inode.set_mtime(0);
        inode.set_mtime_ns(0);
        inode.set_atime(0);
        inode.set_atime_ns(0);
        inode.set_uid(0);
        inode.set_gid(0);
        inode.set_mode(0);
        inode.set_nlink(0);
        inode.set_type(FsFileType::TYPE_FILE);
        return inode;
    }

 protected:
    std::string dirname_;
    std::string pathname_;
};

TEST_F(StorageTest, ContainerIterator) {
    // CASE 1: dentry storage
    auto dentry = GenDentry();
    auto dentryStorage = DentryHash{ {"A", dentry}, {"B", dentry} };
    auto dcontainer = std::shared_ptr<DentryHash>(
        &dentryStorage, [](DentryHash*) {});
    auto diter = MapContainerIterator<DentryHash>(
        ENTRY_TYPE::DENTRY, 100, dcontainer);

    auto size = 0;
    std::string exceptValue;
    ASSERT_TRUE(dentry.SerializeToString(&exceptValue));
    for (diter.SeekToFirst(); diter.Valid(); diter.Next()) {
        ASSERT_EQ(diter.Key(), "d:100");
        ASSERT_EQ(diter.Value(), exceptValue);
        size++;
    }
    ASSERT_EQ(diter.Size(), 2);
    ASSERT_EQ(size, 2);

    // CASE 2: inode storage
    auto inode = GenInode();
    auto inodeStorage = InodeHash{ {"A", inode}, {"B", inode} };
    auto icontainer = std::shared_ptr<InodeHash>(
        &inodeStorage, [](InodeHash*) {});
    auto iiter = MapContainerIterator<InodeHash>(
        ENTRY_TYPE::INODE, 200, icontainer);

    size = 0;
    ASSERT_TRUE(inode.SerializeToString(&exceptValue));
    for (iiter.SeekToFirst(); iiter.Valid(); iiter.Next()) {
        ASSERT_EQ(iiter.Key(), "i:200");
        ASSERT_EQ(iiter.Value(), exceptValue);
        size++;
    }
    ASSERT_EQ(iiter.Size(), 2);
    ASSERT_EQ(size, 2);
}

TEST_F(StorageTest, MergeIterator) {
    auto TEST_MERGE = [&](std::vector<Hash>&& hashs) {
        std::vector<std::pair<std::string, std::string>> except, out;
        std::vector<std::shared_ptr<Iterator>> children;
        for (auto& hash : hashs) {
            children.push_back(std::make_shared<HashIterator>(&hash));
            for (const auto& item : hash) {
                except.push_back(std::make_pair(item.first, item.second));
            }
        }

        auto iter = MergeIterator(children);
        for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
            auto key = iter.Key();
            auto value = iter.Value();
            out.push_back(std::make_pair(key, value));
        }

        ASSERT_EQ(except, out);
    };

    TEST_MERGE(std::vector<Hash>{
        Hash{ { "A0", "A0" }, { "A1", "A1" }, { "A2", "A2" } },
        Hash{ { "A3", "A3" }, { "A4", "A4" }, { "A5", "A5" } },
    });

    TEST_MERGE(std::vector<Hash>{
        Hash{ { "A0", "A0" }, { "A1", "A1" }, { "A2", "A2" } },
        Hash{ { "A3", "A3" } },
    });

    TEST_MERGE(std::vector<Hash>{
        Hash{ { "A0", "A0" }, { "A1", "A1" }, { "A2", "A2" } },
        Hash{ { "A3", "A3" }, { "A4", "A4" }, { "A5", "A5" } },
        Hash{ { "A6", "A6" }, },
    });

    TEST_MERGE(std::vector<Hash>{
        Hash{ { "A0", "0" }, { "A1", "1" }, { "A2", "2" } },
        Hash{ { "A3", "3" } },
    });

    TEST_MERGE(std::vector<Hash>{
        Hash{},
        Hash{ { "A0", "A0" }, { "A1", "A1" }, { "A2", "A2" } },
    });

    TEST_MERGE(std::vector<Hash>{
        Hash{},
    });

    TEST_MERGE(std::vector<Hash>{
        Hash{},
        Hash{ { "A0", "A0" }, { "A1", "A1" }, { "A2", "A2" } },
        Hash{},
    });
}

TEST_F(StorageTest, MergeIteratorSize) {
    auto hash1 = Hash{ { "A0", "A0" } };
    auto hash2 = Hash{ { "B0", "B0" } };
    std::vector<std::shared_ptr<Iterator>> children;
    children.push_back(std::make_shared<HashIterator>(&hash1));
    children.push_back(std::make_shared<HashIterator>(&hash2));
    auto iter = MergeIterator(children);
    ASSERT_EQ(iter.Size(), 2);

    hash1.emplace(std::make_pair("A1", "A1"));
    ASSERT_EQ(iter.Size(), 3);

    uint64_t size = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size++;
    }
    ASSERT_EQ(size, 3);
}

};  // namespace metaserver
};  // namespace curvefs

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);

    ::curvefs::common::Process::InitSetProcTitle(argc, argv);

    return RUN_ALL_TESTS();
}
