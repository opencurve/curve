/*
 * Project: curve
 * Created Date: Thu Sep 05 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
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
