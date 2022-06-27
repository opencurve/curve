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
 * Created Date: 2022-06-27
 * Author: xuchaojie
 */

#ifndef SRC_CHUNKSERVER_EPOCH_MAP_H_
#define SRC_CHUNKSERVER_EPOCH_MAP_H_

#include <map>

#include "proto/chunk.pb.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace chunkserver {

class EpochMap {
 public:
     struct VolumeEpoch{
        uint64_t epoch;
        curve::common::WritePreferedRWLock epochMutex_;
        explicit VolumeEpoch(uint64_t e) : epoch(e) {}
     };

 public:
    EpochMap() {}
    ~EpochMap() {}

    bool UpdateEpoch(uint64_t fileId, uint64_t epoch);

    bool CheckEpoch(uint64_t fileId, uint64_t epoch);

 private:
    // volumeid => epoch
    std::map<uint64_t, std::shared_ptr<VolumeEpoch> > internalMap_;
    curve::common::WritePreferedRWLock mapMutex_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_EPOCH_MAP_H_
