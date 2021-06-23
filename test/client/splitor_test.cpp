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
 * File Created: Mon Jun 21 19:41:55 CST 2021
 * Author: wuhanqing
 */

#include "src/client/splitor.h"

#include <gtest/gtest.h>

#include <tuple>

#include "src/client/io_tracker.h"
#include "src/client/metacache.h"

namespace curve {
namespace client {

struct TestParams {
    FileStatus fileStatus;
    OpType opType;
    uint64_t offset;
    uint64_t length;
    int expectedRequests;
    int expectedUnAlignedRequests;
};

class SplitorAlignmentTest : public ::testing::TestWithParam<TestParams> {
 protected:
    void SetUp() override {
        params_ = GetParam();

        splitOpt_.alignment.commonVolume = 512;
        splitOpt_.alignment.cloneVolume = 4096;
        splitOpt_.fileIOSplitMaxSizeKB = 64;
        Splitor::Init(splitOpt_);

        chunkIdInfo_.lpid_ = 1;
        chunkIdInfo_.cpid_ = 2;
        chunkIdInfo_.cid_ = 3;
        chunkIdInfo_.chunkExist = true;

        metaCache_.SetLatestFileStatus(params_.fileStatus);

        iotracker_ = new IOTracker(nullptr, nullptr, nullptr, nullptr);
        iotracker_->SetOpType(params_.opType);

        if (params_.opType == OpType::WRITE) {
            std::string fakeData(params_.length, 'c');
            writeData_.append(fakeData);
        }
    }

    void TearDown() override {
        delete iotracker_;

        for (auto& r : requests_) {
            r->UnInit();
            delete r;
        }
    }

    int UnalignedRequests() const {
        int count = 0;
        for (auto& r : requests_) {
            count += (r->padding.aligned ? 0 : 1);
        }

        return count;
    }

 protected:
    TestParams params_;
    IOSplitOption splitOpt_;
    ChunkIDInfo chunkIdInfo_;
    MetaCache metaCache_;
    IOTracker* iotracker_;
    butil::IOBuf writeData_;
    std::vector<RequestContext*> requests_;
};

TEST_P(SplitorAlignmentTest, Test) {
    EXPECT_EQ(0, Splitor::SingleChunkIO2ChunkRequests(
                     iotracker_, &metaCache_, &requests_, chunkIdInfo_,
                     params_.opType == OpType::WRITE ? &writeData_ : nullptr,
                     params_.offset, params_.length, 0));

    EXPECT_EQ(params_.expectedRequests, requests_.size());
    EXPECT_EQ(params_.expectedUnAlignedRequests, UnalignedRequests());
}

INSTANTIATE_TEST_CASE_P(
    SplitorTest, SplitorAlignmentTest,
    ::testing::Values(
        // case 1. start offset is aligned, end offset is not aligned
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 0,
                   .length = 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 4096,
                   .length = 4096 + 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 10ull * 1024 * 1024,
                   .length = 64ul * 1024 - 1024,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 8ull * 1024 * 1024,
                   .length = 64ul * 1024 + 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 0,
                   .length = 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 4096,
                   .length = 4096 + 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 10ull * 1024 * 1024,
                   .length = 64ul * 1024 - 1024,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 8ull * 1024 * 1024,
                   .length = 64ul * 1024 + 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 1},

        // case 2. start offset is not aligned, end offset is aligned
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 2048,
                   .length = 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 2048,
                   .length = 4096 + 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 10ull * 1024 * 1024 + 1024,
                   .length = 64ul * 1024 - 1024,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 8ull * 1024 * 1024 - 1024,
                   .length = 64ul * 1024 + 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 2048,
                   .length = 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 2048,
                   .length = 4096 + 2048,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 10ull * 1024 * 1024 + 1024,
                   .length = 64ul * 1024 - 1024,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 8ull * 1024 * 1024 - 1024,
                   .length = 64ul * 1024 + 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 1},

        // case 3. both start offset and end offset are unaligned
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 2048,
                   .length = 1024,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 2048,
                   .length = 4096,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 2 * 1024,
                   .length = 60ull * 1024 - 4 * 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 2},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::WRITE,
                   .offset = 8ull * 1024 * 1024 + 2 * 1024,
                   .length = 128ul * 1024 - 4 * 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 2},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 2048,
                   .length = 1024,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 2048,
                   .length = 4096,
                   .expectedRequests = 1,
                   .expectedUnAlignedRequests = 1},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 2 * 1024,
                   .length = 60ull * 1024 - 4 * 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 2},
        TestParams{.fileStatus = FileStatus::CloneMetaInstalled,
                   .opType = OpType::READ,
                   .offset = 8ull * 1024 * 1024 + 2 * 1024,
                   .length = 128ul * 1024 - 4 * 1024,
                   .expectedRequests = 2,
                   .expectedUnAlignedRequests = 2}));

}  // namespace client
}  // namespace curve

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
