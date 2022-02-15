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

TEST_F(StorageTest, Storage) {
    // step1: generate merge iterator
    auto dentry = GenDentry();
    auto dentryStorage = DentryHash{ {"A", dentry}, {"B", dentry} };
    auto dcontainer = std::shared_ptr<DentryHash>(
        &dentryStorage, [](DentryHash*) {});
    auto diter = std::make_shared<MapContainerIterator<DentryHash>>(
        ENTRY_TYPE::DENTRY, 100, dcontainer);

    auto inode = GenInode();
    auto inodeStorage = InodeHash{ {"A", inode}, {"B", inode} };
    auto icontainer = std::shared_ptr<InodeHash>(
        &inodeStorage, [](InodeHash*) {});
    auto iiter = std::make_shared<MapContainerIterator<InodeHash>>(
        ENTRY_TYPE::INODE, 200, icontainer);

    auto children = std::vector<std::shared_ptr<Iterator>>{ diter, iiter };
    auto miter = std::make_shared<MergeIterator>(children);

    // step2: save to file
    auto succ = SaveToFile(pathname_, miter);
    ASSERT_TRUE(succ);

    // step3: load from file
    auto ndentry = 0;
    auto ninode = 0;
    auto match = true;
    auto callback = [&](ENTRY_TYPE type, uint32_t partitionId, void* entry) {
        if (type == ENTRY_TYPE::DENTRY) {
            auto equal = MessageDifferencer::Equals(
                dentry, *reinterpret_cast<Dentry*>(entry));
            if (!equal || partitionId != 100) {
                return false;
            }
            ndentry++;
        } else if (type == ENTRY_TYPE::INODE) {
            auto equal = MessageDifferencer::Equals(
                inode, *reinterpret_cast<Inode*>(entry));
            if (!equal || partitionId != 200) {
                return false;
            }
            ninode++;
        }
        return true;
    };
    succ = LoadFromFile<decltype(callback)>(pathname_, callback);
    ASSERT_TRUE(succ);
    ASSERT_EQ(ndentry, 2);
    ASSERT_EQ(ninode, 2);
    ASSERT_TRUE(match);
}

};  // namespace metaserver
};  // namespace curvefs

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);

    ::curvefs::common::Process::InitSetProcTitle(argc, argv);

    return RUN_ALL_TESTS();
}
