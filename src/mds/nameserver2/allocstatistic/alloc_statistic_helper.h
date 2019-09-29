/*
 * Project: curve
 * File Created: 20190830
 * Author: lixiaocui
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_HELPER_H_
#define SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_HELPER_H_

#include <map>
#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/mds/kvstorageclient/etcd_client.h"

using ::curve::mds::topology::PoolIdType;

namespace curve {
namespace mds {
extern const int GETBUNDLE;

class AllocStatisticHelper {
 public:
    // 获取记录的physicalPool对应的segment的值
    static int GetExistSegmentAllocValues(
        std::map<PoolIdType, int64_t> *out,
        const std::shared_ptr<EtcdClientImp> &client);

    static int CalculateSegmentAlloc(
        int64_t revision, const std::shared_ptr<EtcdClientImp> &client,
        std::map<PoolIdType, int64_t> *out);
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_HELPER_H_
