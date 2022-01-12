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
 * Created Date: Monday September 10th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_IDGENERATOR_ETCD_ID_GENERATOR_H_
#define SRC_IDGENERATOR_ETCD_ID_GENERATOR_H_

#include <string>
#include <memory>
#include "src/kvstorageclient/etcd_client.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace idgenerator {

using curve::kvstorage::KVStorageClient;

class EtcdIdGenerator {
 public:
    EtcdIdGenerator(const std::shared_ptr<KVStorageClient>& client,
                    const std::string& storeKey, uint64_t initial,
                    uint64_t bundle)
        : storeKey_(storeKey),
          initialize_(initial),
          bundle_(bundle),
          client_(client),
          nextId_(initial),
          bundleEnd_(initial),
          lock_() {}

    ~EtcdIdGenerator() {}


    bool GenID(uint64_t *id);

 private:
    /*
    * @brief apply for IDs in batches from storage
    *
    * @param[in] requiredNum Number of IDs that need to be applied
    *
    * @param[out] false if failed, true if succeeded
    */
    bool AllocateBundleIds(int requiredNum);

 private:
    const std::string storeKey_;
    uint64_t initialize_;
    uint64_t bundle_;

    std::shared_ptr<KVStorageClient> client_;
    uint64_t nextId_;
    uint64_t bundleEnd_;

    curve::common::Mutex lock_;
};

}  // namespace idgenerator
}  // namespace curve

#endif   // SRC_IDGENERATOR_ETCD_ID_GENERATOR_H_
