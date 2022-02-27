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
 * Date: Friday Mar 18 11:29:08 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_UTILS_H_
#define CURVEFS_SRC_CLIENT_VOLUME_UTILS_H_

#include <map>
#include <vector>

#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/src/volume/common.h"
#include "curvefs/src/volume/space_manager.h"

namespace curvefs {

namespace metaserver {
class Inode;
}  // namespace metaserver

namespace client {

using ::curvefs::metaserver::Inode;
using ::curvefs::volume::Extent;
using ::curvefs::volume::SpaceManager;
using ::curvefs::volume::WritePart;

enum {
    kAccessTime = 1 << 0,
    kChangeTime = 1 << 1,
    kModifyTime = 1 << 2,
};

void UpdateInodeTimestamp(Inode* inode, int flags);

/**
 * @brief Allocate space for a write request
 * @param space space manager that used for allocating space
 * @param part allocate info
 * @param[out] writes generated write request, key is file offset
 * @param[out] alloc allocated extents, key is file offset
 * @return return true on success, otherwise return false
 */
bool AllocSpace(SpaceManager* space,
                const AllocPart& part,
                std::map<uint64_t, WritePart>* writes,
                std::map<uint64_t, Extent>* alloc);

bool PrepareWriteRequest(off_t off,
                         size_t size,
                         const char* data,
                         ExtentCache* extentCache,
                         SpaceManager* spaceManager,
                         std::vector<WritePart>* writes);

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_UTILS_H_
