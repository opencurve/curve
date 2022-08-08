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
 * Date: Monday Jun 06 19:18:59 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "curvefs/test/client/rpcclient/mock_metacache.h"

namespace curvefs {
namespace client {
namespace rpcclient {

using ::curvefs::metaserver::MetaStatusCode;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

TEST(CreateInodeTaskExecutorTest, TestPartitionAllocIdFail) {
    auto context = std::make_shared<TaskContext>();
    context->rpctask = [](LogicPoolID poolID, CopysetID copysetID,
                          PartitionID partitionID, uint64_t txId,
                          uint64_t applyIndex, brpc::Channel *channel,
                          brpc::Controller *cntl, TaskExecutorDone *done) {
        static int count = 0;
        ++count;
        if (count == 1) {
            // return PARTITION_ALLOC_ID_FAIL firstly
            return MetaStatusCode::PARTITION_ALLOC_ID_FAIL;
        }

        return MetaStatusCode::OK;
    };

    auto mockMetaCache = std::make_shared<MockMetaCache>();
    auto channelMgr = std::make_shared<ChannelManager<MetaserverID>>();
    CreateInodeExcutor executor(ExcutorOpt{}, mockMetaCache, channelMgr,
                                std::move(context));

    EXPECT_CALL(*mockMetaCache, MarkPartitionUnavailable(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaCache, SelectTarget(_, _, _))
        .Times(2)
        .WillRepeatedly(Invoke(
            [](uint32_t /*fsId*/, CopysetTarget *target, uint64_t *applyIndex) {
                target->groupID = CopysetGroupID{1, 1};
                target->partitionID = 1;
                target->txId = 1;
                target->metaServerID = 1;

                butil::EndPoint ep;
                butil::str2endpoint("127.0.0.1:12345", &ep);

                target->endPoint = std::move(ep);

                *applyIndex = 1;
                return true;
            }));

    EXPECT_EQ(MetaStatusCode::OK, executor.DoRPCTask());
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
