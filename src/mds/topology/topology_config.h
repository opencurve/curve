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
    uint32_t TopologyUpdateToRepoSec;
    uint32_t CreateCopysetRpcTimeoutMs;
    uint32_t CreateCopysetRpcRetryTimes;
    uint32_t CreateCopysetRpcRetrySleepTimeMs;
    uint32_t UpdateMetricIntervalSec;

    TopologyOption()
        : TopologyUpdateToRepoSec(0),
          CreateCopysetRpcTimeoutMs(500),
          CreateCopysetRpcRetryTimes(3),
          CreateCopysetRpcRetrySleepTimeMs(500),
          UpdateMetricIntervalSec(0) {}
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_CONFIG_H_
