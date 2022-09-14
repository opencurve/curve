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
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 */
#include "src/mds/topology/topology_id_generator.h"

namespace curve {
namespace mds {
namespace topology {

void DefaultIdGenerator::initPoolsetIdGenerator(PoolsetIdType idMax) {
    poolsetIdGentor_.init(idMax);
}

void DefaultIdGenerator::initLogicalPoolIdGenerator(PoolIdType idMax) {
  logicPoolIdGentor_.init(idMax);
}

void DefaultIdGenerator::initPhysicalPoolIdGenerator(PoolIdType idMax) {
  physicalPoolIdGentor_.init(idMax);
}

void DefaultIdGenerator::initZoneIdGenerator(ZoneIdType idMax) {
  zoneIdGentor_.init(idMax);
}

void DefaultIdGenerator::initServerIdGenerator(ServerIdType idMax) {
  serverIdGentor_.init(idMax);
}

void DefaultIdGenerator::initChunkServerIdGenerator(ChunkServerIdType idMax) {
  chunkserverIdGentor_.init(idMax);
}

void DefaultIdGenerator::initCopySetIdGenerator(
    const std::map<PoolIdType, CopySetIdType> &idMaxMap) {
  copySetIdGentor_.clear();
  for (auto it : idMaxMap) {
    copySetIdGentor_[it.first].init(it.second);
  }
}
PoolsetIdType DefaultIdGenerator::GenPoolsetId() {
    return poolsetIdGentor_.GenId();
}

PoolIdType DefaultIdGenerator::GenLogicalPoolId() {
  return logicPoolIdGentor_.GenId();
}

PoolIdType DefaultIdGenerator::GenPhysicalPoolId() {
  return physicalPoolIdGentor_.GenId();
}

ZoneIdType DefaultIdGenerator::GenZoneId() {
  return zoneIdGentor_.GenId();
}

ServerIdType DefaultIdGenerator::GenServerId() {
  return serverIdGentor_.GenId();
}

ChunkServerIdType DefaultIdGenerator::GenChunkServerId() {
  return chunkserverIdGentor_.GenId();
}

CopySetIdType DefaultIdGenerator::GenCopySetId(PoolIdType logicalPoolId) {
  return copySetIdGentor_[logicalPoolId].GenId();
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
