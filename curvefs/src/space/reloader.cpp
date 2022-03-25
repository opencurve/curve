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

/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/space/reloader.h"

#include <brpc/channel.h>
#include <glog/logging.h>

#include "curvefs/src/space/metaserver_client.h"

namespace curvefs {
namespace space {

bool Reloader::Reload() {
    MetaServerClient client;
    if (client.Init(option_.metaServerOption) == false) {
        LOG(ERROR) << "init metaserver client failed";
        return false;
    }

    Extents exts;
    if (!client.GetAllInodeExtents(fsId_, rootInodeId_, &exts)) {
        LOG(ERROR) << "get all inode extents failed";
        return false;
    }

    allocator_->MarkUsed(exts);

    return true;
}

}  // namespace space
}  // namespace curvefs
