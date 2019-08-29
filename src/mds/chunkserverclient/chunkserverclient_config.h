/*
 * Project: curve
 * Created Date: Fri Aug 30 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVERCLIENT_CONFIG_H_
#define SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVERCLIENT_CONFIG_H_

#include <string>

namespace curve {
namespace mds {
namespace chunkserverclient {

struct ChunkServerClientOption {
    uint32_t rpcTimeoutMs;
    uint32_t rpcRetryTimes;
    uint32_t rpcRetryIntervalMs;
    uint32_t updateLeaderRetryTimes;
    uint32_t updateLeaderRetryIntervalMs;
    ChunkServerClientOption()
        : rpcTimeoutMs(500),
          rpcRetryTimes(10),
          rpcRetryIntervalMs(500),
          updateLeaderRetryTimes(3),
          updateLeaderRetryIntervalMs(5000) {}
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVERCLIENT_CONFIG_H_
