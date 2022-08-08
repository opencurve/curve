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
 * Date: Monday Mar 14 19:27:02 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_EXTENT_CACHE_H_
#define CURVEFS_SRC_CLIENT_VOLUME_EXTENT_CACHE_H_

#include <bvar/bvar.h>

#include <cstring>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/volume/extent.h"
#include "curvefs/src/client/volume/extent_slice.h"
#include "curvefs/src/volume/common.h"
#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace client {

using ::curvefs::volume::ReadPart;
using ::curvefs::volume::WritePart;
using ::curvefs::metaserver::VolumeExtentSlice;
using ::curvefs::metaserver::VolumeExtentList;

struct ExtentCacheOption {
    // preallocation size if offset ~ length is not allocated
    // TODO(wuhanqing): preallocation size should take care of file size
    uint64_t preAllocSize = 64ULL * 1024;
    // a single file's extents are split by offset, and each one called `slice`
    uint64_t sliceSize = 1ULL * 1024 * 1024 * 1024;
    // minimum allocate and read/write unit
    uint32_t blockSize = 4096;
};

class ExtentCache {
 public:
    ExtentCache() = default;

    static void SetOption(const ExtentCacheOption& option);

    void Build(const VolumeExtentList& extents);

    void DivideForWrite(uint64_t offset,
                        uint64_t len,
                        const char* data,
                        std::vector<WritePart>* allocated,
                        std::vector<AllocPart>* needAlloc);

    void DivideForRead(uint64_t offset,
                       uint64_t len,
                       char* data,
                       std::vector<ReadPart>* reads,
                       std::vector<ReadPart>* holes);

    void Merge(uint64_t loffset, const PExtent& pExt);

    void MarkWritten(uint64_t offset, uint64_t len);

    bool HasDirtyExtents() const;

    VolumeExtentList GetDirtyExtents();

    std::unordered_map<uint64_t, std::map<uint64_t, PExtent>>
    GetExtentsForTesting() const;

 private:
    static void DivideForWriteWithinEmptySlice(
        uint64_t offset,
        uint64_t len,
        const char* data,
        std::vector<AllocPart>* needAlloc);

 private:
    mutable curve::common::RWLock lock_;

    // key is offset
    std::unordered_map<uint64_t, ExtentSlice> slices_;

    // dirty slices
    std::unordered_set<ExtentSlice*> dirties_;

 private:
    friend class ExtentSlice;

    static ExtentCacheOption option_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_EXTENT_CACHE_H_
