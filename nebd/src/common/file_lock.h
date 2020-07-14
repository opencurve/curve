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

#ifndef NEBD_SRC_COMMON_FILE_LOCK_H_
#define NEBD_SRC_COMMON_FILE_LOCK_H_

#include <string>

namespace nebd {
namespace common {

// 文件锁
class FileLock {
 public:
    explicit FileLock(const std::string& fileName)
      : fileName_(fileName), fd_(-1) {}

    FileLock() : fileName_(""), fd_(-1) {}
    ~FileLock() = default;

    /**
     * @brief 获取文件锁
     * @return 成功返回0，失败返回-1
     */
    int AcquireFileLock();


    /**
     * @brief 释放文件锁
     */
    void ReleaseFileLock();

 private:
    // 锁文件的文件名
    std::string fileName_;
    // 锁文件的fd
    int fd_;
};

}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_FILE_LOCK_H_
