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
#include "curvefs/src/metaserver/storage/iterator.h"
#include "curvefs/src/metaserver/storage/storage_fstream.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using Hash = std::unordered_map<std::string, std::string>;
using ContainerType = std::map<std::string, std::string>;
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
    bool ParseFromValue(ValueType* value) { return true; }
    int Status() override { return 0; }

 private:
    Hash* hash_;
    Hash::iterator iter_;
};

class IteratorTest : public ::testing::Test {
 protected:
    IteratorTest()
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

TEST_F(IteratorTest, ContainerIterator) {
    // CASE 1: dentry
    {
        std::string exceptValue;
        auto dentry = GenDentry();
        ASSERT_TRUE(dentry.SerializeToString(&exceptValue));

        auto container = std::make_shared<ContainerType>();
        container->emplace("key", exceptValue);
        auto iterator = ContainerIterator<ContainerType>(container);

        auto size = 0;
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next()) {
            ASSERT_EQ(iterator.Key(), "key");
            ASSERT_EQ(iterator.Value(), exceptValue);
            size++;
        }
        ASSERT_EQ(iterator.Size(), 1);
        ASSERT_EQ(size, 1);
    }

    // CASE 2: indoe
    {
        std::string exceptValue;
        auto inode = GenInode();
        ASSERT_TRUE(inode.SerializeToString(&exceptValue));

        auto container = std::make_shared<ContainerType>();
        container->emplace("key", exceptValue);
        auto iterator = ContainerIterator<ContainerType>(container);

        auto size = 0;
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next()) {
            ASSERT_EQ(iterator.Key(), "key");
            ASSERT_EQ(iterator.Value(), exceptValue);
            size++;
        }
        ASSERT_EQ(iterator.Size(), 1);
        ASSERT_EQ(size, 1);
    }
}

TEST_F(IteratorTest, MergeIterator) {
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

TEST_F(IteratorTest, MergeIteratorSize) {
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

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
