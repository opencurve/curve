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

#ifndef CURVEFS_SRC_MDS_SCHEDULE_OPERATORSTEP_H_
#define CURVEFS_SRC_MDS_SCHEDULE_OPERATORSTEP_H_

#include <cstdint>
#include <string>
#include "curvefs/src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operatorStepTemplate.h"

namespace curvefs {
namespace mds {
namespace schedule {
using OperatorStep =
    curve::mds::schedule::OperatorStepT<MetaServerIdType, CopySetInfo,
                                           CopySetConf>;
using RemovePeer =
    curve::mds::schedule::RemovePeerT<MetaServerIdType, CopySetInfo,
                                         CopySetConf>;
using AddPeer = curve::mds::schedule::AddPeerT<MetaServerIdType, CopySetInfo,
                                                  CopySetConf>;
using TransferLeader =
    curve::mds::schedule::TransferLeaderT<MetaServerIdType, CopySetInfo,
                                             CopySetConf>;
using ChangePeer =
    curve::mds::schedule::ChangePeerT<MetaServerIdType, CopySetInfo,
                                         CopySetConf>;
using CancelScanPeer =
    curve::mds::schedule::CancelScanPeerT<MetaServerIdType, CopySetInfo,
                                             CopySetConf>;
using StartScanPeer =
    curve::mds::schedule::StartScanPeerT<MetaServerIdType, CopySetInfo,
                                            CopySetConf>;
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_OPERATORSTEP_H_
