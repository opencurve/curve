/*
 * Project: curve
 * Created Date: Thu Jun 20 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/mds/topology/topology_stat.h"

#include <memory>
#include <set>
#include <utility>

#include "src/common/concurrent/concurrent.h"

using ::curve::common::WriteLockGuard;
using ::curve::common::ReadLockGuard;

namespace curve {
namespace mds {
namespace topology {

void TopologyStatImpl::UpdateChunkServerStat(ChunkServerIdType csId,
    const ChunkServerStat &stat) {
    WriteLockGuard wLock(statsLock_);
    auto it = chunkServerStats_.find(csId);
    if (it != chunkServerStats_.end()) {
        it->second = stat;
    } else {
        chunkServerStats_.emplace(csId, stat);
    }
    return;
}

bool TopologyStatImpl::GetChunkServerStat(ChunkServerIdType csId,
    ChunkServerStat *stat) {
    ReadLockGuard rLock(statsLock_);
    auto it = chunkServerStats_.find(csId);
    if (it != chunkServerStats_.end()) {
        *stat = it->second;
        return true;
    }
    return false;
}

int TopologyStatImpl::Init() {
    return kTopoErrCodeSuccess;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
