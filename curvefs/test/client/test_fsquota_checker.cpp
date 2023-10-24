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

#include <gtest/gtest.h>

#include "curvefs/src/client/rpcclient/fsdelta_updater.h"
#include "curvefs/src/client/rpcclient/fsquota_checker.h"

namespace curvefs {
namespace client {

using testing::Matcher;

class FsQuotaCheckerTest : public testing::Test {
 protected:
    virtual void SetUp() {
        FsDeltaUpdater::GetInstance().Init();
        FsQuotaChecker::GetInstance().Init();
    }
    virtual void TearDown() {}
};

TEST_F(FsQuotaCheckerTest, QuotaBytesCheck) {
    // case 0: fsused bytes correct
    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(100);
    ASSERT_EQ(FsDeltaUpdater::GetInstance().GetDeltaBytes(), 100);
    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(-100);
    ASSERT_EQ(FsDeltaUpdater::GetInstance().GetDeltaBytes(), 0);

    // case 1: quota disabled
    FsQuotaChecker::GetInstance().UpdateQuotaCache(0, 0);
    ASSERT_TRUE(FsQuotaChecker::GetInstance().QuotaBytesCheck(100));

    // case 2: incBytes > capacity
    FsQuotaChecker::GetInstance().UpdateQuotaCache(1, 1);
    ASSERT_FALSE(FsQuotaChecker::GetInstance().QuotaBytesCheck(2));

    // case 3: capacity - usedBytes < incBytes
    FsQuotaChecker::GetInstance().UpdateQuotaCache(2, 1);
    ASSERT_FALSE(FsQuotaChecker::GetInstance().QuotaBytesCheck(3));

    // case 4: capacity - usedBytes < localDelta + incBytes
    FsQuotaChecker::GetInstance().UpdateQuotaCache(4, 0);
    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(1);
    ASSERT_FALSE(FsQuotaChecker::GetInstance().QuotaBytesCheck(4));
    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(-1);

    // case 5: capacity - usedBytes >= localDelta + incBytes
    FsQuotaChecker::GetInstance().UpdateQuotaCache(5, 1);
    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(1);
    ASSERT_TRUE(FsQuotaChecker::GetInstance().QuotaBytesCheck(3));
    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(-1);
}

}  // namespace client
}  // namespace curvefs
