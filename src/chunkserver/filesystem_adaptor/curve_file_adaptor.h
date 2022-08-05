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
 * Created Date: 2020-06-10
 * Author: charisu
 */

#ifndef SRC_CHUNKSERVER_FILESYSTEM_ADAPTOR_CURVE_FILE_ADAPTOR_H_
#define SRC_CHUNKSERVER_FILESYSTEM_ADAPTOR_CURVE_FILE_ADAPTOR_H_

#include <braft/file_system_adaptor.h>

#include "src/fs/local_filesystem.h"

using curve::fs::LocalFileSystem;

namespace curve {
namespace chunkserver {

class CurveFileAdaptor : public braft::FileAdaptor {
 public:
    explicit CurveFileAdaptor(int fd,
        const std::shared_ptr<LocalFileSystem> &lfs)
        : fd_(fd), lfs_(lfs) {}

    ssize_t write(const butil::IOBuf& data, off_t offset) override;
    ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    ssize_t size() override;
    bool sync() override;
    bool close() override;

 private:
    int fd_;
    std::shared_ptr<LocalFileSystem> lfs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_FILESYSTEM_ADAPTOR_CURVE_FILE_ADAPTOR_H_
