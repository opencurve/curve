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

#ifndef CURVEFS_SRC_CLIENT_EXTENT_MANAGER_H_
#define CURVEFS_SRC_CLIENT_EXTENT_MANAGER_H_

#include <list>
#include <cstdint>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/extent.h"
#include "curvefs/src/client/error_code.h"

using ::curvefs::metaserver::VolumeExtentList;
using ::curvefs::space::Extent;

namespace curvefs {
namespace client {



/**
 * @brief  According to the extents in the file,
 * divide the [offset, len] to be written,
 * and get the extents that need to be allocated
 *
 * @param[in] extents  extents in the file
 * @param[in] offset  offset to be written
 * @param[in] len  len to be written
 * @param[out] toAllocExtents  extents that need to be allocated
 *
 * @return errcode
 */
CURVEFS_ERROR GetToAllocExtents(const VolumeExtentList &extents,
    uint64_t offset,
    uint64_t len,
    std::list<ExtentAllocInfo> *toAllocExtents);

/**
 * @brief  Merge extents allocated to extents in the file
 *
 * @param[in] allocedExtents  extents allocated
 * @param[in] toAllocExtents  extents that need to be allocated
 * @param[in,out] extents  extents in the file
 *
 * @return  errcode
 */
CURVEFS_ERROR MergeAllocedExtents(
    const std::list<ExtentAllocInfo> &toAllocExtents,
    const std::list<Extent> allocatedExtents,
    VolumeExtentList *extents);

/**
 * @brief mark unwritten extents to written in the file extents
 *
 * @param[in] offset  offset to be written
 * @param[in] len  len to be written
 * @param[in,out] extents  extents in the file
 *
 * @return  errcode
 */
CURVEFS_ERROR MarkExtentsWritten(uint64_t offset, uint64_t len,
    VolumeExtentList *extents);

/**
 * @brief According to the extents in the file,
 * divide the [lOffset, len] to be read/write,
 * and get the physical extents
 *
 * @param extents  extents in the file
 * @param offset  logical offset to be read/write
 * @param len  len to be read/write
 * @param PExtent  physical extents
 *
 * @return errcode
 */
CURVEFS_ERROR DivideExtents(const VolumeExtentList &extents,
    uint64_t lOffset,
    uint64_t len,
    std::list<PExtent> *pExtents);



}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_EXTENT_MANAGER_H_
