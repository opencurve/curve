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
#include "nebd/src/common/rw_lock.h"

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
 * @brief: Save the information of the currently opened file
 */
class NebdClientMetaCache {
 public:
    NebdClientMetaCache() = default;
    ~NebdClientMetaCache() = default;

    /**
     * @brief: Add file information
     * @param: fileInfo file information
     */
    void AddFileInfo(const NebdClientFileInfo& fileInfo);

    /**
     * @brief: Delete file information
     * @param: fd file descriptor
     */
    void RemoveFileInfo(int fd);

    /**
     * @brief: Obtain the file information of the corresponding fd
     * @param: fd file fd
     * @param[out]: fileInfo
     * @return: 0 succeeded/-1 returned
     */
    int GetFileInfo(int fd, NebdClientFileInfo* fileInfo) const;

    /**
     * @brief: Get information about currently opened files
     * @return: Currently opened file information
     */
    std::vector<NebdClientFileInfo> GetAllFileInfo() const;

 private:
    //Currently opened file information
    std::unordered_map<int, NebdClientFileInfo> fileinfos_;
    mutable nebd::common::RWLock rwLock_;
};

}  // namespace client
}  // namespace nebd

#endif  // NEBD_SRC_PART1_NEBD_METACACHE_H_
