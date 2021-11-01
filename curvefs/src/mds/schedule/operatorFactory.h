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

#include <cstdint>
#include <vector>
#include "curvefs/src/mds/schedule/operator.h"
#include "curvefs/src/mds/schedule/operatorStep.h"
#include "curvefs/src/mds/schedule/topoAdapter.h"

#ifndef CURVEFS_SRC_MDS_SCHEDULE_OPERATORFACTORY_H_
#define CURVEFS_SRC_MDS_SCHEDULE_OPERATORFACTORY_H_
namespace curvefs {
namespace mds {
namespace schedule {

class OperatorFactory {
 public:
    /**
     * @brief Create_xxxx_Operator: Create operator for xxxx
     */
    Operator CreateTransferLeaderOperator(const CopySetInfo &info,
                                          MetaServerIdType newLeader,
                                          OperatorPriority pri);

    Operator CreateRemovePeerOperator(const CopySetInfo &info,
                                      MetaServerIdType rmPeer,
                                      OperatorPriority pri);

    Operator CreateAddPeerOperator(const CopySetInfo &info,
                                   MetaServerIdType addPeer,
                                   OperatorPriority pri);

    Operator CreateChangePeerOperator(const CopySetInfo &info,
                                      MetaServerIdType rmPeer,
                                      MetaServerIdType addPeer,
                                      OperatorPriority pri);
};

extern OperatorFactory operatorFactory;

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_SCHEDULE_OPERATORFACTORY_H_
