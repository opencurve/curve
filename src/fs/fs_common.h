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

#ifndef SRC_FS_FS_COMMON_H_
#define SRC_FS_FS_COMMON_H_

#include <string>

namespace curve {
namespace fs {

enum class FileSystemType {
    // SFS,
    EXT4,
    PFS,
    UNKOWN,
};

inline static FileSystemType StringToFileSystemType(
    const std::string &typeStr) {
    FileSystemType type;
    if (typeStr == "ext4") {
        type = FileSystemType::EXT4;
    } else if (typeStr == "pfs") {
        type = FileSystemType::PFS;
    } else {
        type = FileSystemType::UNKOWN;
    }
    return type;
}

struct FileSystemInfo {
    uint64_t total = 0;         // Total bytes
    uint64_t available = 0;     // Free bytes available for unprivileged users
    uint64_t allocated = 0;     // Bytes allocated by the store
    uint64_t stored = 0;        // Bytes actually stored by the user
};

}  // namespace fs
}  // namespace curve
#endif  // SRC_FS_FS_COMMON_H_
