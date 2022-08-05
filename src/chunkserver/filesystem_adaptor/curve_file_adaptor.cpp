/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-08-24
 * Author: xuchaojie
 */

#include "src/chunkserver/filesystem_adaptor/curve_file_adaptor.h"

namespace braft {
DECLARE_bool(raft_use_fsync_rather_than_fdatasync);
}

namespace curve {
namespace chunkserver {

ssize_t CurveFileAdaptor::write(const butil::IOBuf& data, off_t offset) {
    return lfs_->Write(fd_, data, offset, data.size());
}

ssize_t CurveFileAdaptor::read(
    butil::IOPortal* portal, off_t offset, size_t size) {
    return lfs_->Read(fd_, portal, offset, size);
}

ssize_t CurveFileAdaptor::size() {
    off_t sz = lfs_->Lseek(fd_, 0, SEEK_END);
    return ssize_t(sz);
}

bool CurveFileAdaptor::sync() {
    if (braft::FLAGS_raft_use_fsync_rather_than_fdatasync) {
        return lfs_->Fsync(fd_) == 0;
    } else {
        return lfs_->Fdatasync(fd_) == 0;
    }
}

bool CurveFileAdaptor::close() {
    if (!sync()) {
        return false;
    }
    if (fd_ < 0) {
        return true;
    }
    return lfs_->Close(fd_) == 0;
}

}  // namespace chunkserver
}  // namespace curve
