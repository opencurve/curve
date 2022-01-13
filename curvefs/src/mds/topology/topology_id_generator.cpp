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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */
#include "curvefs/src/mds/topology/topology_id_generator.h"

namespace curvefs {
namespace mds {
namespace topology {
void DefaultIdGenerator::initPoolIdGenerator(PoolIdType idMax) {
  PoolIdGentor_.init(idMax);
}

void DefaultIdGenerator::initZoneIdGenerator(ZoneIdType idMax) {
  zoneIdGentor_.init(idMax);
}

void DefaultIdGenerator::initServerIdGenerator(ServerIdType idMax) {
  serverIdGentor_.init(idMax);
}

void DefaultIdGenerator::initMetaServerIdGenerator(MetaServerIdType idMax) {
  metaserverIdGentor_.init(idMax);
}

void DefaultIdGenerator::initCopySetIdGenerator(
    const std::map<PoolIdType, CopySetIdType> &idMaxMap) {
  copySetIdGentor_.clear();
  for (const auto &it : idMaxMap) {
    copySetIdGentor_[it.first].init(it.second);
  }
}

void DefaultIdGenerator::initPartitionIdGenerator(PartitionIdType idMax) {
  PartitionIdGentor_.init(idMax);
}

PoolIdType DefaultIdGenerator::GenPoolId() {
  return PoolIdGentor_.GenId();
}

ZoneIdType DefaultIdGenerator::GenZoneId() {
  return zoneIdGentor_.GenId();
}

ServerIdType DefaultIdGenerator::GenServerId() {
  return serverIdGentor_.GenId();
}

MetaServerIdType DefaultIdGenerator::GenMetaServerId() {
  return metaserverIdGentor_.GenId();
}

CopySetIdType DefaultIdGenerator::GenCopySetId(PoolIdType poolId) {
  return copySetIdGentor_[poolId].GenId();
}

PartitionIdType DefaultIdGenerator::GenPartitionId() {
  return PartitionIdGentor_.GenId();
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

