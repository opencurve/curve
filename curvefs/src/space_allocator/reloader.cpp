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

#include "curvefs/src/space_allocator/reloader.h"

#include <brpc/channel.h>
#include <glog/logging.h>

#include <vector>

#include "curvefs/src/space_allocator/metaserver_client.h"

namespace curvefs {
namespace space {

DefaultRealoder::DefaultRealoder(Allocator* allocator, uint64_t fsId,
                                 uint64_t rootInodeId)
    : Reloader(allocator), fsId_(fsId), rootInodeId_(rootInodeId) {}

DefaultRealoder::~DefaultRealoder() {}

bool DefaultRealoder::Reload() {
    MetaServerClientOption opt;
    MetaServerClient client;
    if (client.Init(opt) == false) {
        LOG(ERROR) << "init metaserver client failed";
        return false;
    }

    std::vector<PExtent> exts;
    if (client.GetAllInodeExtents(fsId_, rootInodeId_, &exts) == false) {
        LOG(ERROR) << "get all inode extents failed";
        return false;
    }

    allocator_->MarkUsed(exts);

    return true;
}

}  // namespace space
}  // namespace curvefs
