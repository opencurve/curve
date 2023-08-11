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
 * Date: Mon Apr 24 20:04:24 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_MOCK_PARTITION_H_
#define CURVEFS_TEST_METASERVER_MOCK_MOCK_PARTITION_H_

#include <gmock/gmock.h>
#include <vector>
#include "curvefs/src/metaserver/partition.h"

namespace curvefs {
namespace metaserver {
namespace mock {
class MockPartition : public curvefs::metaserver::Partition {
 public:
    MockPartition() : Partition() {}
    MOCK_METHOD1(GetAllBlockGroup,
                 MetaStatusCode(std::vector<DeallocatableBlockGroup>*));
    MOCK_CONST_METHOD0(GetPartitionId, uint32_t());
    MOCK_CONST_METHOD0(GetFsId, uint32_t());
};
}  // namespace mock
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_MOCK_PARTITION_H_
