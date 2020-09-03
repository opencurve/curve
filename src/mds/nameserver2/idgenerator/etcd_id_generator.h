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

#ifndef SRC_MDS_NAMESERVER2_IDGENERATOR_ETCD_ID_GENERATOR_H_
#define SRC_MDS_NAMESERVER2_IDGENERATOR_ETCD_ID_GENERATOR_H_

#include <string>
#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::Atomic;
using ::curve::kvstorage::KVStorageClient;

namespace curve {
namespace mds {
class EtcdIdGenerator {
 public:
    EtcdIdGenerator(
        const std::shared_ptr<KVStorageClient> &client,
        const std::string &storeKey, uint64_t initial, uint64_t bundle) :
        client_(client), storeKey_(storeKey), initialize_(initial),
        bundle_(bundle), nextId_(initial), bundleEnd_(initial) {}
    virtual ~EtcdIdGenerator() {}


    bool GenID(InodeID *id);

 private:
    /**
    * @brief apply for IDs in batches from storage
    *
    * @param[in] requiredNum Number of IDs that need to be applied
    *
    * @param[out] false if failed, true if succeeded
    */
    bool AllocateBundleIds(int requiredNum);

 private:
    std::string storeKey_;
    uint64_t initialize_;
    uint64_t bundle_;

    std::shared_ptr<KVStorageClient> client_;
    uint64_t nextId_;
    uint64_t bundleEnd_;

    ::curve::common::RWLock lock_;
};

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_IDGENERATOR_ETCD_ID_GENERATOR_H_
