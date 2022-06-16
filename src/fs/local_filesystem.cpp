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
 * File Created: 18-10-31
 * Author: yangyaokai
 */

#include <glog/logging.h>

#include "src/fs/local_filesystem.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/fs/wrap_posix.h"
#include "src/fs/pfs_filesystem_impl.h"

namespace curve {
namespace fs {

std::shared_ptr<LocalFileSystem> LocalFsFactory::CreateFs(
    FileSystemType type,
    const std::string& deviceID) {
    std::shared_ptr<LocalFileSystem> localFs;
    switch (type) {
    case FileSystemType::EXT4:
        localFs = Ext4FileSystemImpl::getInstance();
        break;
    case FileSystemType::PFS:
        localFs = PfsFileSystemImpl::getInstance();
        break;
    default:
        LOG(ERROR) << "Unknown filesystem type";
        return nullptr;
    }
    return localFs;
}

}  // namespace fs
}  // namespace curve

