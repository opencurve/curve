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
 * Created Date: Thu Jun 18 2020
 * Author: xuchaojie
 */

#ifndef TEST_MDS_TOPOLOGY_TEST_TOPOLOGY_HELPER_H_
#define TEST_MDS_TOPOLOGY_TEST_TOPOLOGY_HELPER_H_

#include "src/mds/topology/topology_item.h"

namespace curve {
namespace mds {
namespace topology {
bool JudgePoolsetEqual(const Poolset &lh, const Poolset &rh);

bool JudgeLogicalPoolEqual(const LogicalPool &lh, const LogicalPool &rh);

bool JudgePhysicalPoolEqual(const PhysicalPool &lh, const PhysicalPool &rh);

bool JudgeZoneEqual(const Zone &lh, const Zone &rh);


bool JudgeServerEqual(const Server &lh, const Server &rh);

bool JudgeChunkServerEqual(const ChunkServer &lh, const ChunkServer &rh);

bool JudgeCopysetInfoEqual(const CopySetInfo &lh, const CopySetInfo &rh);

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_TOPOLOGY_TEST_TOPOLOGY_HELPER_H_
