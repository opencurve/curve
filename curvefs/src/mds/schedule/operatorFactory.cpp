/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/operatorFactory.h"
#include <cstdint>
#include <memory>

using ::std::chrono::steady_clock;

namespace curvefs {
namespace mds {
namespace schedule {
OperatorFactory operatorFactory;

Operator OperatorFactory::CreateTransferLeaderOperator(
    const CopySetInfo &info, MetaServerIdType newLeader, OperatorPriority pri) {
    return Operator(info.epoch, info.id, pri, steady_clock::now(),
                    std::make_shared<TransferLeader>(info.leader, newLeader));
}
Operator OperatorFactory::CreateRemovePeerOperator(const CopySetInfo &info,
                                                   MetaServerIdType peer,
                                                   OperatorPriority pri) {
    return Operator(info.epoch, info.id, pri, steady_clock::now(),
                    std::make_shared<RemovePeer>(peer));
}

Operator OperatorFactory::CreateAddPeerOperator(const CopySetInfo &info,
                                                MetaServerIdType addPeer,
                                                OperatorPriority pri) {
    return Operator(info.epoch, info.id, pri, steady_clock::now(),
                    std::make_shared<AddPeer>(addPeer));
}

Operator OperatorFactory::CreateChangePeerOperator(const CopySetInfo &info,
                                                   MetaServerIdType rmPeer,
                                                   MetaServerIdType addPeer,
                                                   OperatorPriority pri) {
    return Operator(info.epoch, info.id, pri, steady_clock::now(),
                    std::make_shared<ChangePeer>(rmPeer, addPeer));
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
