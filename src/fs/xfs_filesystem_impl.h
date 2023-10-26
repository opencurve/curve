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

#ifndef SRC_FS_XFS_FILESYSTEM_IMPL_H_
#define SRC_FS_XFS_FILESYSTEM_IMPL_H_


#include "src/fs/ext4_filesystem_impl.h"


namespace curve {
namespace fs {
class XfsFileSystemImpl : public Ext4FileSystemImpl {
 public:
    XfsFileSystemImpl(std::shared_ptr<PosixWrapper> posixWrapper) : Ext4FileSystemImpl(posixWrapper) {
            posixWrapper_ = posixWrapper;
            enableRenameat2_ = false;
            enableReflink_ = false;
    }
    ~XfsFileSystemImpl() {}
    static std::shared_ptr<XfsFileSystemImpl> getInstance() ;

    int WriteWithClone(int fd,
                        int src_fd,
                        uint64_t src_offset,
                        int src_length,
                        uint64_t dest_offset);

 private:
    static std::shared_ptr<XfsFileSystemImpl> self_;
    static std::mutex mutex_;
    std::shared_ptr<PosixWrapper> posixWrapper_;
    bool enableRenameat2_;
    bool enableReflink_;
};

}  // namespace fs
}  // namespace curve

#endif  // SRC_FS_XFS_FILESYSTEM_IMPL_H_
