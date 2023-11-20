/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-12-07
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_IDGENERATOR_TS_ID_GENERATOR_H_
#define CURVEFS_SRC_MDS_IDGENERATOR_TS_ID_GENERATOR_H_

#include "curvefs/src/mds/common/storage_key.h"
#include "src/idgenerator/etcd_id_generator.h"

namespace curvefs {
namespace  mds {

class TsIdGenerator {
 public:
    explicit TsIdGenerator(
        const std::shared_ptr<curve::kvstorage::KVStorageClient>& client)
        : generator_(new curve::idgenerator::EtcdIdGenerator(
              client, TS_INFO_KEY_PREFIX, TS_ID_INIT, TS_ID_ALLOCATE_BUNDLE)) {}

    bool GenTsId(uint64_t* id) {
        return generator_->GenID(id);
    }

 private:
    static constexpr uint64_t TS_ID_INIT = 0;
    static constexpr uint64_t TS_ID_ALLOCATE_BUNDLE = 100;

 private:
    std::unique_ptr<curve::idgenerator::EtcdIdGenerator> generator_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_IDGENERATOR_TS_ID_GENERATOR_H_
