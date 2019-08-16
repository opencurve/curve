/*
 * Project: curve
 * Created Date: Wed May 08 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_

#include <string>

namespace curve {
namespace mds {
namespace topology {

struct TopologyOption {
    // topology更新至数据库时间间隔
    uint32_t TopologyUpdateToRepoSec;
    // 创建copyset rpc超时时间
    uint32_t CreateCopysetRpcTimeoutMs;
    // 创建copyset rpc超时重试次数
    uint32_t CreateCopysetRpcRetryTimes;
    // 创建copyset rpc超时重试时间间隔
    uint32_t CreateCopysetRpcRetrySleepTimeMs;
    // 更新topology metric 时间间隔
    uint32_t UpdateMetricIntervalSec;
    //  物理池使用百分比，
    //  即使用量超过这个值即不再往这个池分配
    uint32_t PoolUsagePercentLimit;
    // ChoosePoolPolicy
    int choosePoolPolicy;

    TopologyOption()
        : TopologyUpdateToRepoSec(0),
          CreateCopysetRpcTimeoutMs(500),
          CreateCopysetRpcRetryTimes(3),
          CreateCopysetRpcRetrySleepTimeMs(500),
          UpdateMetricIntervalSec(0),
          PoolUsagePercentLimit(100),
          choosePoolPolicy(0) {}
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
