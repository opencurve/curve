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
 * Created Date: Wed Nov 28 2018
 * Author: lixiaocui
 */

#include "src/mds/schedule/operatorFactoryTemplate.h"
#include "src/mds/schedule/topoAdapter.h"

#ifndef SRC_MDS_SCHEDULE_OPERATORFACTORY_H_
#define SRC_MDS_SCHEDULE_OPERATORFACTORY_H_
namespace curve {
namespace mds {
namespace schedule {
using OperatorFactory =
    OperatorFactoryT<ChunkServerIdType, CopySetInfo, CopySetConf>;
extern OperatorFactory operatorFactory;
}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_SCHEDULE_OPERATORFACTORY_H_
