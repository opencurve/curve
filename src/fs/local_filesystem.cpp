/*
 * Project: curve
 * File Created: 18-10-31
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>

#include "src/fs/local_filesystem.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/fs/wrap_posix.h"

namespace curve {
namespace fs {

std::shared_ptr<LocalFileSystem> LocalFsFactory::CreateFs(
    FileSystemType type,
    const std::string& deviceID) {
    std::shared_ptr<LocalFileSystem> localFs;
    if (type == FileSystemType::EXT4) {
        localFs = Ext4FileSystemImpl::getInstance();
    } else {
        LOG(ERROR) << "Unknown filesystem type.";
        return nullptr;
    }
    return localFs;
}

}  // namespace fs
}  // namespace curve

