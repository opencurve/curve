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
 * Created Date: Thu Sep 05 2019
 * Author: xuchaojie
 */

#ifndef TEST_MDS_MOCK_MOCK_ALLOC_STATISTIC_H_
#define TEST_MDS_MOCK_MOCK_ALLOC_STATISTIC_H_

#include <gmock/gmock.h>
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"

namespace curve {
namespace mds {

class MockAllocStatistic : public AllocStatistic {
 public:
    MockAllocStatistic() :
        AllocStatistic(0, 0, nullptr) {}

    MOCK_METHOD2(GetAllocByLogicalPool,
        bool(PoolIdType lid, int64_t *alloc));
    MOCK_METHOD3(AllocSpace,
        void(PoolIdType, int64_t changeSize, int64_t revision));
    MOCK_METHOD3(DeAllocSpace,
        void(PoolIdType, int64_t changeSize, int64_t revision));
};

}  // namespace mds
}  // namespace curve


#endif  // TEST_MDS_MOCK_MOCK_ALLOC_STATISTIC_H_
