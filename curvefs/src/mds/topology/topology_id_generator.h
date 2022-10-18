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
#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_ID_GENERATOR_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_ID_GENERATOR_H_

#include <cstdint>
#include <list>
#include <algorithm>
#include <memory>
#include <map>
#include <atomic>

#include "curvefs/src/mds/common/mds_define.h"

namespace curvefs {
namespace mds {
namespace topology {

class TopologyIdGenerator {
 public:
    TopologyIdGenerator() {}
    virtual ~TopologyIdGenerator() {}

    virtual void initPoolIdGenerator(PoolIdType idMax) = 0;
    virtual void initZoneIdGenerator(ZoneIdType idMax) = 0;
    virtual void initServerIdGenerator(ServerIdType idMax) = 0;
    virtual void initMetaServerIdGenerator(MetaServerIdType idMax) = 0;
    virtual void initCopySetIdGenerator(
        const std::map<PoolIdType, CopySetIdType> &idMaxMap) = 0;
    virtual void initPartitionIdGenerator(PartitionIdType idMax) = 0;
    virtual void initMemcacheClusterIdGenerator(
        MemcacheClusterIdType idMax) = 0;

    virtual PoolIdType GenPoolId() = 0;
    virtual ZoneIdType GenZoneId() = 0;
    virtual ServerIdType GenServerId() = 0;
    virtual MetaServerIdType GenMetaServerId() = 0;
    virtual CopySetIdType GenCopySetId(PoolIdType PoolId) = 0;
    virtual PartitionIdType GenPartitionId() = 0;
    virtual MemcacheClusterIdType GenMemCacheClusterId() = 0;
};


class DefaultIdGenerator : public TopologyIdGenerator {
 public:
    DefaultIdGenerator() {}
    ~DefaultIdGenerator() {}

    virtual void initPoolIdGenerator(PoolIdType idMax);
    virtual void initZoneIdGenerator(ZoneIdType idMax);
    virtual void initServerIdGenerator(ServerIdType idMax);
    virtual void initMetaServerIdGenerator(MetaServerIdType idMax);
    virtual void initCopySetIdGenerator(const std::map<PoolIdType,
        CopySetIdType> &idMaxMap);
    virtual void initPartitionIdGenerator(PartitionIdType idMax);
    virtual void initMemcacheClusterIdGenerator(MemcacheClusterIdType idMax);

    virtual PoolIdType GenPoolId();
    virtual ZoneIdType GenZoneId();
    virtual ServerIdType GenServerId();
    virtual MetaServerIdType GenMetaServerId();
    virtual CopySetIdType GenCopySetId(PoolIdType PoolId);
    virtual PartitionIdType GenPartitionId();
    virtual MemcacheClusterIdType GenMemCacheClusterId();

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

    IdGenerator<PoolIdType> PoolIdGentor_;
    IdGenerator<ZoneIdType> zoneIdGentor_;
    IdGenerator<ServerIdType> serverIdGentor_;
    IdGenerator<MetaServerIdType> metaserverIdGentor_;
    std::map<PoolIdType, IdGenerator<CopySetIdType> > copySetIdGentor_;
    IdGenerator<PartitionIdType> PartitionIdGentor_;
    IdGenerator<MemcacheClusterIdType> memcacheClusterIdGentor_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_ID_GENERATOR_H_
