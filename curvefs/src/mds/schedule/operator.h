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

#ifndef CURVEFS_SRC_MDS_SCHEDULE_OPERATOR_H_
#define CURVEFS_SRC_MDS_SCHEDULE_OPERATOR_H_

#include "src/mds/schedule/operatorTemplate.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorStep.h"
#include "curvefs/src/mds/schedule/topoAdapter.h"
#include "curvefs/src/mds/topology/topology_item.h"

namespace curvefs {
namespace mds {
namespace schedule {
using curve::mds::schedule::OperatorPriority;
using Operator = curve::mds::schedule::OperatorT<MetaServerIdType,
                                                    CopySetInfo, CopySetConf>;
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_SCHEDULE_OPERATOR_H_
