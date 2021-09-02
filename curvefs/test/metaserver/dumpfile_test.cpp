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
 * Created Date: 2021-07-27
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <string>
#include <memory>
#include <thread>
#include <unordered_map>

#include "curvefs/src/common/process.h"
#include "curvefs/src/metaserver/iterator.h"
#include "curvefs/src/metaserver/dumpfile.h"

namespace curvefs {
namespace metaserver {

using Hash = std::unordered_map<std::string, std::string>;

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

class DumpFileTest : public ::testing::Test {
 protected:
    DumpFileTest()
        : dirname_(".dumpfile") {}

    void SetUp() override {
        std::string ret;
        ASSERT_TRUE(ExecShell("mkdir -p " + dirname_, &ret));
        dumpfile_ = std::make_shared<DumpFile>(dirname_ + "/curvefs.dump");
        ASSERT_EQ(dumpfile_->Open(), DUMPFILE_ERROR::OK);
    }

    void TearDown() override {
        ASSERT_EQ(dumpfile_->Close(), DUMPFILE_ERROR::OK);
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

    void CheckIterator(std::shared_ptr<Iterator> iter, Hash* hash) {
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            auto key = iter->Key();
            auto value = iter->Value();
            ASSERT_EQ(key, value);
            ASSERT_TRUE(hash->find(key) != hash->end());
            hash->erase(key);
        }

        ASSERT_EQ(hash->size(), 0);
    }

    void GenHash(Hash* hash, uint64_t count) {
        for (auto i = 1; i <= count; i++) {
            auto num = std::to_string(i);
            hash->emplace(num, num);
        }
    }

 protected:
    std::string dirname_;
    std::shared_ptr<DumpFile> dumpfile_;
};

TEST_F(DumpFileTest, TestSaveBasic) {
    Hash hash;
    auto hashIterator = std::make_shared<HashIterator>(&hash);

    // CASE 1: save
    ASSERT_EQ(hash.size(), 0);
    GenHash(&hash, 1);

    ASSERT_EQ(dumpfile_->Save(hashIterator), DUMPFILE_ERROR::OK);
    CheckIterator(dumpfile_->Load(), &hash);
    ASSERT_EQ(dumpfile_->GetLoadStatus(), DUMPFILE_LOAD_STATUS::COMPLETE);

    // CASE 2: save background
    ASSERT_EQ(hash.size(), 0);
    GenHash(&hash, 10);

    ASSERT_EQ(dumpfile_->SaveBackground(hashIterator), DUMPFILE_ERROR::OK);
    CheckIterator(dumpfile_->Load(), &hash);
    ASSERT_EQ(dumpfile_->GetLoadStatus(), DUMPFILE_LOAD_STATUS::COMPLETE);
}

TEST_F(DumpFileTest, TestSaveBinaryData) {
    Hash hash;
    auto hashIterator = std::make_shared<HashIterator>(&hash);

    // Generate string which include binary data
    std::string key;
    char bytes[] = "a\0\xFF\101";
    auto length = 1000;
    for (auto i = 1; i <= length; i++) {
        key += bytes[i % (sizeof(bytes) - 1)];
    }

    ASSERT_EQ(key.size(), length);
    for (auto i = 1; i <= 3; i++) {
        hash.emplace(key, key);
    }

    ASSERT_EQ(dumpfile_->SaveBackground(hashIterator), DUMPFILE_ERROR::OK);
    CheckIterator(dumpfile_->Load(), &hash);
    ASSERT_EQ(dumpfile_->GetLoadStatus(), DUMPFILE_LOAD_STATUS::COMPLETE);
}

TEST_F(DumpFileTest, TestSaveInTherad) {
    Hash hash;
    auto hashIterator = std::make_shared<HashIterator>(&hash);

    GenHash(&hash, 10);

    auto savaThread = std::thread([&](){
        ASSERT_EQ(dumpfile_->SaveBackground(hashIterator), DUMPFILE_ERROR::OK);
    });

    savaThread.join();
    CheckIterator(dumpfile_->Load(), &hash);
    ASSERT_EQ(dumpfile_->GetLoadStatus(), DUMPFILE_LOAD_STATUS::COMPLETE);
}

// TEST_F(DumpFileTest, TestSaveWithSet) {
//     Hash hash;
//     auto hashIterator = std::make_shared<HashIterator>(&hash);

//     volatile bool running = true;

//     auto pushThread = std::thread([&](){
//         int i = 0;
//         while (running) {
//             auto key = std::to_string(++i);
//             hash.emplace(key, key);
//         }
//     });

//     std::this_thread::sleep_for(std::chrono::seconds(3));

//     auto saveThread = std::thread([&](){
//         ASSERT_EQ(dumpfile_->SaveBackground(hashIterator), DUMPFILE_ERROR::OK);  // NOLINT
//     });

//     std::this_thread::sleep_for(std::chrono::seconds(1));

//     running = false;
//     pushThread.join();
//     saveThread.join();

//     // Check the dump file
//     std::set<int> set;
//     auto iter = dumpfile_->Load();
//     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
//         ASSERT_EQ(iter->Key(), iter->Value());
//         set.insert(std::stoi(iter->Key()));
//     }
//     ASSERT_GE(set.size(), 0);
//     ASSERT_LE(set.size(), hash.size());
//     ASSERT_EQ(set.size(), iter->Size());

//     int last = 0;
//     for (auto& i : set) {
//         ASSERT_EQ(i, last + 1);
//         last = i;
//     }
// }

TEST_F(DumpFileTest, TestSaveBigData) {
    Hash hash;
    auto hashIterator = std::make_shared<HashIterator>(&hash);

    GenHash(&hash, 5000000);

    ASSERT_EQ(dumpfile_->SaveBackground(hashIterator), DUMPFILE_ERROR::OK);
    CheckIterator(dumpfile_->Load(), &hash);
    ASSERT_EQ(dumpfile_->GetLoadStatus(), DUMPFILE_LOAD_STATUS::COMPLETE);
}

TEST_F(DumpFileTest, TestFileNotOpen) {
    Hash hash;
    auto hashIterator = std::make_shared<HashIterator>(&hash);

    auto dumpfile = DumpFile(".dump/curvefs.dump");
    ASSERT_EQ(dumpfile.Save(hashIterator), DUMPFILE_ERROR::BAD_FD);
    ASSERT_EQ(dumpfile.SaveBackground(hashIterator), DUMPFILE_ERROR::BAD_FD);
    ASSERT_EQ(dumpfile.Close(), DUMPFILE_ERROR::OK);
}

};  // namespace metaserver
};  // namespace curvefs

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);

    ::curvefs::common::Process::InitSetProcTitle(argc, argv);

    return RUN_ALL_TESTS();
}
