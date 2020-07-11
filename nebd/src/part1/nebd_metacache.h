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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#ifndef NEBD_SRC_PART1_NEBD_METACACHE_H_
#define NEBD_SRC_PART1_NEBD_METACACHE_H_

#include <unordered_map>
#include <vector>
#include <string>

#include "nebd/src/common/file_lock.h"
#include "src/common/concurrent/rw_lock.h"

namespace nebd {
namespace client {

using nebd::common::FileLock;

struct NebdClientFileInfo {
    int fd;
    std::string fileName;
    FileLock fileLock;

    NebdClientFileInfo() = default;

    NebdClientFileInfo(
        int fd, const std::string& fileName,
        const FileLock& fileLock)
        : fd(fd),
          fileName(fileName),
          fileLock(fileLock) {}
};

/**
 * @brief: 保存当前已打开文件信息
 */
class NebdClientMetaCache {
 public:
    NebdClientMetaCache() = default;
    ~NebdClientMetaCache() = default;

    /**
     * @brief: 添加文件信息
     * @param: fileInfo 文件信息
     */
    void AddFileInfo(const NebdClientFileInfo& fileInfo);

    /**
     * @brief: 删除文件信息
     * @param: fd 文件描述符
     */
    void RemoveFileInfo(int fd);

    /**
     * @brief: 获取对应fd的文件信息
     * @param: fd 文件fd
     * @param[out]: fileInfo
     * @return: 0 成功 / -1 返回
     */
    int GetFileInfo(int fd, NebdClientFileInfo* fileInfo) const;

    /**
     * @brief: 获取当前已打开文件信息
     * @return: 当前已打开文件信息
     */
    std::vector<NebdClientFileInfo> GetAllFileInfo() const;

 private:
    // 当前已打开文件信息
    std::unordered_map<int, NebdClientFileInfo> fileinfos_;
    mutable curve::common::RWLock rwLock_;
};

}  // namespace client
}  // namespace nebd

#endif  // NEBD_SRC_PART1_NEBD_METACACHE_H_
