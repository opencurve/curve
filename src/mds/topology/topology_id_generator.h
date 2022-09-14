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
#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_ID_GENERATOR_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_ID_GENERATOR_H_

#include <cstdint>
#include <list>
#include <algorithm>
#include <memory>
#include <map>
#include <atomic>

#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {
namespace topology {

class TopologyIdGenerator {
 public:
    TopologyIdGenerator() {}
    virtual ~TopologyIdGenerator() {}

    virtual void initPoolsetIdGenerator(PoolsetIdType idMax) = 0;
    virtual void initLogicalPoolIdGenerator(PoolIdType idMax) = 0;
    virtual void initPhysicalPoolIdGenerator(PoolIdType idMax) = 0;
    virtual void initZoneIdGenerator(ZoneIdType idMax) = 0;
    virtual void initServerIdGenerator(ServerIdType idMax) = 0;
    virtual void initChunkServerIdGenerator(ChunkServerIdType idMax) = 0;
    virtual void initCopySetIdGenerator(
        const std::map<PoolIdType, CopySetIdType> &idMaxMap) = 0;

    virtual PoolsetIdType GenPoolsetId() = 0;
    virtual PoolIdType GenLogicalPoolId() = 0;
    virtual PoolIdType GenPhysicalPoolId() = 0;
    virtual ZoneIdType GenZoneId() = 0;
    virtual ServerIdType GenServerId() = 0;
    virtual ChunkServerIdType GenChunkServerId() = 0;
    virtual CopySetIdType GenCopySetId(PoolIdType logicalPoolId) = 0;
};


class DefaultIdGenerator : public TopologyIdGenerator {
 public:
    DefaultIdGenerator() {}
    ~DefaultIdGenerator() {}

    virtual void initPoolsetIdGenerator(PoolsetIdType idMax);
    virtual void initLogicalPoolIdGenerator(PoolIdType idMax);
    virtual void initPhysicalPoolIdGenerator(PoolIdType idMax);
    virtual void initZoneIdGenerator(ZoneIdType idMax);
    virtual void initServerIdGenerator(ServerIdType idMax);
    virtual void initChunkServerIdGenerator(ChunkServerIdType idMax);
    virtual void initCopySetIdGenerator(const std::map<PoolIdType,
        CopySetIdType> &idMaxMap);
    virtual PoolsetIdType GenPoolsetId();
    virtual PoolIdType GenLogicalPoolId();
    virtual PoolIdType GenPhysicalPoolId();
    virtual ZoneIdType GenZoneId();
    virtual ServerIdType GenServerId();
    virtual ChunkServerIdType GenChunkServerId();
    virtual CopySetIdType GenCopySetId(PoolIdType logicalPoolId);

 private:
    template <typename T>
    class IdGenerator {
     public:
        IdGenerator() : idMax_(0) {}
        ~IdGenerator() {}

        void init(T idMax) {
            idMax_.store(idMax);
        }

        T GenId() {
            return ++idMax_;
        }


     private:
        std::atomic<T> idMax_;
    };

    IdGenerator<PoolsetIdType> poolsetIdGentor_;
    IdGenerator<PoolIdType> logicPoolIdGentor_;
    IdGenerator<PoolIdType> physicalPoolIdGentor_;
    IdGenerator<ZoneIdType> zoneIdGentor_;
    IdGenerator<ServerIdType> serverIdGentor_;
    IdGenerator<ChunkServerIdType> chunkserverIdGentor_;
    std::map<PoolIdType, IdGenerator<CopySetIdType> > copySetIdGentor_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_ID_GENERATOR_H_
