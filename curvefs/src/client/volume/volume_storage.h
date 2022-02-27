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
 * Date: Monday Mar 14 17:40:12 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_VOLUME_STORAGE_H_
#define CURVEFS_SRC_CLIENT_VOLUME_VOLUME_STORAGE_H_

#include <sys/types.h>

#include <cstddef>
#include <cstdint>

namespace curvefs {
namespace client {

class VolumeStorage {
 public:
    virtual ~VolumeStorage() = default;

    virtual ssize_t Read(uint64_t ino,
                         off_t offset,
                         size_t len,
                         char* data) = 0;

    virtual ssize_t Write(uint64_t ino,
                          off_t offset,
                          size_t len,
                          const char* data) = 0;

    virtual bool Flush(uint64_t ino) = 0;

    virtual bool Shutdown() = 0;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_VOLUME_STORAGE_H_
