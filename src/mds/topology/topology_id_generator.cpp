/*
 * Project: curve
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */
#include "src/mds/topology/topology_id_generator.h"

namespace curve {
namespace mds {
namespace topology {
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

