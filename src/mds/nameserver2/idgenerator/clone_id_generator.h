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
 * Created Date: 2023-06-13
 * Author: xuchaojie
 */

#ifndef SRC_MDS_NAMESERVER2_IDGENERATOR_CLONE_ID_GENERATOR_H_
#define SRC_MDS_NAMESERVER2_IDGENERATOR_CLONE_ID_GENERATOR_H_

#include <string>
#include <memory>

#include "src/common/namespace_define.h"
#include "src/mds/nameserver2/idgenerator/etcd_id_generator.h"

using ::curve::common::CLONEIDSTOREKEY;

namespace curve {
namespace  mds {
const uint64_t CLONEIDINITIALIZE = 0;
const uint64_t CLONEIDBUNDLEALLOCATED = 1000;

class CloneIDGenerator {
 public:
     virtual bool GenCloneID(uint64_t *id) = 0;
};

class CloneIDGeneratorImpl : public CloneIDGenerator {
 public:
    explicit CloneIDGeneratorImpl(std::shared_ptr<KVStorageClient> client) {
        generator_ = std::make_shared<EtcdIdGenerator>(
            client, CLONEIDSTOREKEY, CLONEIDINITIALIZE, CLONEIDBUNDLEALLOCATED);
    }

    bool GenCloneID(uint64_t *id) override {
        return generator_->GenID(id);
    }

 private:
    std::shared_ptr<EtcdIdGenerator> generator_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_IDGENERATOR_CLONE_ID_GENERATOR_H_
