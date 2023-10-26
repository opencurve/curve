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
#include <sys/vfs.h>
#include <sys/utsname.h>
#include <linux/version.h>
#include <dirent.h>
#include <brpc/server.h>

#include "src/common/string_util.h"
#include "src/fs/xfs_filesystem_impl.h"
#include "src/fs/wrap_posix.h"

#define MIN_KERNEL_VERSION KERNEL_VERSION(3, 15, 0)

namespace curve {
namespace fs {

std::shared_ptr<XfsFileSystemImpl> XfsFileSystemImpl::self_ = nullptr;
std::mutex XfsFileSystemImpl::mutex_;

std::shared_ptr<XfsFileSystemImpl> XfsFileSystemImpl::getInstance() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (self_ == nullptr) {
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<PosixWrapper>();
        self_ = std::shared_ptr<XfsFileSystemImpl>(
                new(std::nothrow) XfsFileSystemImpl(wrapper));
        CHECK(self_ != nullptr) << "Failed to new xfs local fs.";
    }
    return self_;
}

int XfsFileSystemImpl::WriteWithClone(int fd,
                                    int src_fd,
                                    uint64_t src_offset,
                                    int src_length,
                                    uint64_t dest_offset) {

    int ret = posixWrapper_->ficlonerange(fd,
                                        src_fd,
                                        src_offset,
                                        src_length,
                                        dest_offset);
    if (ret < 0) {
        LOG(ERROR) << "ficlonerange failed: " << strerror(errno);
        return -errno;
    }

    return ret;
}

}  // namespace fs
}  // namespace curve
