/*
 * Project: curve
 * Created Date: Tue Dec 18 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <cstdint>
#include "src/mds/schedule/operatorFactory.h"

using ::std::chrono::steady_clock;

namespace curve {
namespace mds {
namespace schedule {
OperatorFactory operatorFactory;

Operator OperatorFactory::CreateTransferLeaderOperator(
    const CopySetInfo &info,
    ChunkServerIdType newLeader,
    OperatorPriority pri) {
    return Operator(info.epoch, info.id, pri, steady_clock::now(),
                    std::make_shared<TransferLeader>(info.leader, newLeader));
}
Operator OperatorFactory::CreateRemovePeerOperator(
    const CopySetInfo &info, ChunkServerIdType peer, OperatorPriority pri) {
    return Operator(info.epoch, info.id, pri, steady_clock::now(),
                    std::make_shared<RemovePeer>(peer));
}

Operator OperatorFactory::CreateAddPeerOperator(
    const CopySetInfo &info, ChunkServerIdType addPeer, OperatorPriority pri) {
    return Operator(info.epoch,
                    info.id,
                    pri,
                    steady_clock::now(),
                    std::make_shared<AddPeer>(addPeer));
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
