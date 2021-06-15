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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_METASERVER_CLIENT_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_METASERVER_CLIENT_H_

#include <brpc/channel.h>

#include <string>
#include <vector>

#include "curvefs/src/space_allocator/common.h"

namespace curvefs {
namespace space {

struct MetaServerClientOption {
    std::string addr;
};

class MetaServerClient {
 public:
    MetaServerClient() = default;

    ~MetaServerClient() = default;

    bool Init(const MetaServerClientOption& opt);

    bool GetAllInodeExtents(uint32_t fsId, uint64_t rootInodeId,
                            std::vector<PExtent>* exts);

 private:
    bool RecursiveListDentry(uint32_t fsId, uint64_t inodeId,
                             std::vector<PExtent>* exts);

 private:
    MetaServerClientOption opt_;
    brpc::Channel channel_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_METASERVER_CLIENT_H_
