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
 * Created Date: Fri Apr 21 2023
 * Author: Xinlong-Chen
 */

#include <gtest/gtest.h>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/metric/fs_metric.h"

namespace curvefs {
namespace mds {

class MdsMetricTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MdsMetricTest, UpdateFsUsage) {
    FsUsage usage;
    usage.set_usedbytes(100);
    FsMetric::GetInstance().SetFsUsage("test", usage);
    usage.set_usedbytes(200);
    FsMetric::GetInstance().SetFsUsage("test", usage);
    FsMetric::GetInstance().DeleteFsUsage("test");
    FsMetric::GetInstance().DeleteFsUsage("test");
}

}  // namespace mds
}  // namespace curvefs
