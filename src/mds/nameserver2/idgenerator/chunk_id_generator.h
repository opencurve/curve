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
 * Created Date: Saturday October 13th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_IDGENERATOR_CHUNK_ID_GENERATOR_H_
#define SRC_MDS_NAMESERVER2_IDGENERATOR_CHUNK_ID_GENERATOR_H_

#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/common/namespace_define.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/nameserver2/idgenerator/etcd_id_generator.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

using ::curve::common::Atomic;
using ::curve::common::CHUNKSTOREKEY;

namespace curve {
namespace  mds {
const uint64_t CHUNKINITIALIZE = 0;
const uint64_t CHUNKBUNDLEALLOCATED = 1000;

class ChunkIDGenerator {
 public:
    virtual  ~ChunkIDGenerator() {}

    /*
    * @brief GenChunkID Generate a globally incremented ID
    *
    * @param[out] ID generated
    *
    * @return true if succeeded, false if failed
    */
    virtual bool GenChunkID(ChunkID *id) = 0;
};

class ChunkIDGeneratorImp : public ChunkIDGenerator {
 public:
    explicit ChunkIDGeneratorImp(std::shared_ptr<KVStorageClient> client) {
        generator_ = std::make_shared<EtcdIdGenerator>(
            client, CHUNKSTOREKEY, CHUNKINITIALIZE, CHUNKBUNDLEALLOCATED);
    }
    virtual ~ChunkIDGeneratorImp() {}

    bool GenChunkID(ChunkID *id) override;

 private:
    std::shared_ptr<EtcdIdGenerator> generator_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_IDGENERATOR_CHUNK_ID_GENERATOR_H_
