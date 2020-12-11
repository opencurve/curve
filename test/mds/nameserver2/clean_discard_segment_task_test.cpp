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
 * Date: Tue Dec 22 14:25:52 CST 2020
 */

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "test/mds/mock/mock_alloc_statistic.h"
#include "test/mds/mock/mock_chunkserverclient.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/nameserver2/mock/mock_clean_manager.h"
#include "test/mds/nameserver2/mock/mock_namespace_storage.h"

namespace curve {
namespace mds {

using ::curve::mds::chunkserverclient::ChunkServerClientOption;
using ::curve::mds::chunkserverclient::MockChunkServerClient;
using curve::mds::topology::MockTopology;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

class CleanDiscardSegmentTaskTest : public ::testing::Test {
 public:
    void SetUp() override {
        storage_ = std::make_shared<MockNameServerStorage>();
        topology_ = std::make_shared<MockTopology>();
        channelPool_ = std::make_shared<ChannelPool>();
        client_ =
            std::make_shared<CopysetClient>(topology_, option_, channelPool_);
        allocStatistic_ = std::make_shared<MockAllocStatistic>();
        cleanCore_ =
            std::make_shared<CleanCore>(storage_, client_, allocStatistic_);

        csClient_ = std::make_shared<MockChunkServerClient>(topology_, option_,
                                                            channelPool_);

        cleanManager_ = std::make_shared<MockCleanManager>();
    }

    void TearDown() override {}

 protected:
    std::shared_ptr<MockNameServerStorage> storage_;
    std::shared_ptr<MockTopology> topology_;
    ChunkServerClientOption option_;
    std::shared_ptr<ChannelPool> channelPool_;
    std::shared_ptr<CopysetClient> client_;
    std::shared_ptr<MockAllocStatistic> allocStatistic_;
    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<MockChunkServerClient> csClient_;
    std::shared_ptr<MockCleanManager> cleanManager_;
};

TEST_F(CleanDiscardSegmentTaskTest, CommonTest) {
    CleanDiscardSegmentTask task(cleanManager_, storage_, 50);
    std::map<std::string, DiscardSegmentInfo> discardSegments;
    discardSegments.emplace("hello", DiscardSegmentInfo());

    EXPECT_CALL(*storage_, ListDiscardSegment(_))
        .WillRepeatedly(
            DoAll(SetArgPointee<0>(discardSegments),
                  Return(StoreStatus::OK)));

    EXPECT_CALL(*cleanManager_, SubmitCleanDiscardSegmentJob(_, _))
        .WillRepeatedly(Return(true));

    ASSERT_TRUE(task.Start());
    ASSERT_FALSE(task.Start());

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_NO_THROW(task.Stop());
    ASSERT_FALSE(task.Stop());
}

TEST_F(CleanDiscardSegmentTaskTest, TestListDiscardSegmentFailed) {
    CleanDiscardSegmentTask task(cleanManager_, storage_, 50);
    std::map<std::string, DiscardSegmentInfo> discardSegments;
    discardSegments.emplace("hello", DiscardSegmentInfo());

    EXPECT_CALL(*storage_, ListDiscardSegment(_))
        .WillRepeatedly(
            Return(StoreStatus::InternalError));

    EXPECT_CALL(*cleanManager_, SubmitCleanDiscardSegmentJob(_, _))
        .Times(0);

    task.Start();

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_NO_THROW(task.Stop());
}

TEST_F(CleanDiscardSegmentTaskTest, TestSubmitJobFailed) {
    CleanDiscardSegmentTask task(cleanManager_, storage_, 50);
    std::map<std::string, DiscardSegmentInfo> discardSegments;
    discardSegments.emplace("hello", DiscardSegmentInfo());

    EXPECT_CALL(*storage_, ListDiscardSegment(_))
        .WillRepeatedly(
            DoAll(SetArgPointee<0>(discardSegments),
                  Return(StoreStatus::OK)));

    EXPECT_CALL(*cleanManager_, SubmitCleanDiscardSegmentJob(_, _))
        .WillRepeatedly(Return(false));

    task.Start();

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_NO_THROW(task.Stop());
}

}  // namespace mds
}  // namespace curve
