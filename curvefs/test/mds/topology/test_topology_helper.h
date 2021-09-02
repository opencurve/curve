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
 * Project: curve
 * Created Date: 2021-09-05
 * Author: wanghai01
 */

#ifndef CURVEFS_TEST_MDS_TOPOLOGY_TEST_TOPOLOGY_HELPER_H_
#define CURVEFS_TEST_MDS_TOPOLOGY_TEST_TOPOLOGY_HELPER_H_

#include "curvefs/src/mds/topology/topology_item.h"

namespace curvefs {
namespace mds {
namespace topology {

bool ComparePool(const Pool &lh, const Pool &rh);

bool CompareZone(const Zone &lh, const Zone &rh);

bool CompareServer(const Server &lh, const Server &rh);

bool CompareMetaServer(const MetaServer &lh, const MetaServer &rh);

bool CompareCopysetInfo(const CopySetInfo &lh, const CopySetInfo &rh);

bool ComparePartition(const Partition &lh, const Partition &rh);

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_TOPOLOGY_TEST_TOPOLOGY_HELPER_H_
