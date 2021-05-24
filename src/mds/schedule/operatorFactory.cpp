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
 * Created Date: Tue Dec 18 2018
 * Author: lixiaocui
 */

#include <cstdint>
#include <memory>
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
    return Operator(
        info.epoch,
        info.id,
        pri,
        steady_clock::now(),
        std::make_shared<TransferLeader>(info.leader, newLeader));
}
Operator OperatorFactory::CreateRemovePeerOperator(
    const CopySetInfo &info, ChunkServerIdType peer, OperatorPriority pri) {
    return Operator(
        info.epoch,
        info.id,
        pri,
        steady_clock::now(),
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

Operator OperatorFactory::CreateChangePeerOperator(const CopySetInfo &info,
    ChunkServerIdType rmPeer, ChunkServerIdType addPeer,
    OperatorPriority pri) {
    return Operator(
        info.epoch,
        info.id,
        pri,
        steady_clock::now(),
        std::make_shared<ChangePeer>(rmPeer, addPeer));
}

Operator OperatorFactory::CreateScanPeerOperator(const CopySetInfo& info,
                                                 ChunkServerIdType scanPeer,
                                                 OperatorPriority pri,
                                                 ConfigChangeType opType) {
    return Operator(
        info.epoch,
        info.id,  // CopySetKey (logical pool id, copyset id)
        pri,
        steady_clock::now(),
        std::make_shared<ScanPeer>(scanPeer, opType));
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
