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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */
#include "curvefs/src/client/extent_manager.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR GetToAllocExtents(const VolumeExtentList &extents,
    uint64_t offset,
    uint64_t len,
    std::list<ExtentAllocInfo> *toAllocExtents) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MergeAllocedExtents(
    const std::list<ExtentAllocInfo> &toAllocExtents,
    const std::list<Extent> allocatedExtents,
    VolumeExtentList *extents) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MarkExtentsWritten(uint64_t offset, uint64_t len,
    VolumeExtentList *extents) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DivideExtents(const VolumeExtentList &extents,
    uint64_t lOffset,
    uint64_t len,
    std::list<PExtent> *pExtents) {
    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs
