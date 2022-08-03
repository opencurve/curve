/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 22-04-21
 * Author: huyao (baijiaruo)
 */

#ifndef CURVEFS_SRC_CLIENT_UNDER_STORAGE_H_
#define CURVEFS_SRC_CLIENT_UNDER_STORAGE_H_

namespace curvefs {
namespace client {

class UnderStorage {
 public:
    UnderStorage() {}
    virtual ~UnderStorage() = default;
    /**
     * @brief write data from under storage
     */
    ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
    /**
     * @brief read data from under storage and return nonexistent read part
     */
    ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data, std::vector<ReadPart> *miss) = 0; 
     /**
     * @brief truncate data from under storage
     */
    CURVEFS_ERROR Truncate(uint64_t ino, size_t size) = 0;               
};

}
}
#endif  // CURVEFS_SRC_CLIENT_UNDER_STORAGE_H_
 