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
 * Date: Fri May 05 11:42:53 CST 2023
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include "curvefs/src/metaserver/mds/fsinfo_manager.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"

using ::curvefs::client::rpcclient::MockMdsClient;

using ::testing::_;

namespace curvefs {
namespace metaserver {

TEST(FsInfoManagerTest, TestGetFsInfo) {
    auto &manager = FsInfoManager::GetInstance();

    auto mdsCli = std::make_shared<MockMdsClient>();
    manager.SetMdsClient(mdsCli);
    uint32_t fsId = 1;

    EXPECT_CALL(*mdsCli, GetFsInfo(fsId, _))
        .WillOnce(Return(FSStatusCode::NOT_FOUND));
    FsInfo fsInfo;
    ASSERT_FALSE(manager.GetFsInfo(fsId, &fsInfo));

    EXPECT_CALL(*mdsCli, GetFsInfo(fsId, _))
        .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));
    ASSERT_FALSE(manager.GetFsInfo(fsId, &fsInfo));

    EXPECT_CALL(*mdsCli, GetFsInfo(fsId, _)).WillOnce(Return(FSStatusCode::OK));
    ASSERT_TRUE(manager.GetFsInfo(fsId, &fsInfo));
}

}  // namespace metaserver
}  // namespace curvefs
