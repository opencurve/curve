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
 * Project: Curve
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_PACKAGE_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_PACKAGE_H_

#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/xattr_manager.h"

namespace curvefs {
namespace client {
namespace filesystem {

struct ExternalMember {  // external member depended by FileSystem
    ExternalMember(std::shared_ptr<InodeCacheManager> inodeManager,
                   std::shared_ptr<DentryCacheManager> dentryManager,
                   std::shared_ptr<XattrManager> xattrManager)
        : inodeManager(inodeManager),
          dentryManager(dentryManager),
          xattrManager(xattrManager) {}

    std::shared_ptr<InodeCacheManager> inodeManager;
    std::shared_ptr<DentryCacheManager> dentryManager;
    std::shared_ptr<XattrManager> xattrManager;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_PACKAGE_H_
