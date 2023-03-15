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
 * Created Date: 2023-03-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_RPC_CLIENT_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_RPC_CLIENT_H_

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

class RPCClient {
 public:
    RPCClient(RPCOption option,
              std::shared_ptr<InodeCacheManager> inodeManager,
              std::shared_ptr<DentryCacheManager> dentryManager);

    CURVEFS_ERROR GetAttr(Ino ino, InodeAttr* attr);

    CURVEFS_ERROR Lookup(Ino parent, const std::string& name, DirEntry* out);

    CURVEFS_ERROR ReadDir(Ino ino, std::shared_ptr<DirEntryList>* entries);

    CURVEFS_ERROR Open(Ino ino, std::shared_ptr<InodeWrapper>* inode);

 private:
    RPCOption option_;
    std::shared_ptr<InodeCacheManager> inodeManager_;
    std::shared_ptr<DentryCacheManager> dentryManager_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_RPC_CLIENT_H_