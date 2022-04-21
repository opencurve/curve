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
 * Date: Wednesday Apr 20 14:08:35 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_EXTENT_SLICE_H_
#define CURVEFS_SRC_CLIENT_VOLUME_EXTENT_SLICE_H_

#include <map>
#include <vector>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/volume/extent.h"
#include "curvefs/src/volume/common.h"

namespace curvefs {
namespace client {

using ::curvefs::metaserver::VolumeExtentSlice;
using ::curvefs::volume::ReadPart;
using ::curvefs::volume::WritePart;

class ExtentSlice {
 public:
    explicit ExtentSlice(uint64_t offset);

    explicit ExtentSlice(const VolumeExtentSlice& slice);

    void DivideForWrite(uint64_t offset,
                        uint64_t len,
                        const char* data,
                        std::vector<WritePart>* allocated,
                        std::vector<AllocPart>* needAlloc) const;

    void DivideForRead(uint64_t offset,
                       uint64_t len,
                       char* data,
                       std::vector<ReadPart>* reads,
                       std::vector<ReadPart>* holes) const;

    void Merge(uint64_t loffset, const PExtent& extent);

    // mark [off ~ len] written, return whether internal state is changed or not
    bool MarkWritten(uint64_t offset, uint64_t len);

    VolumeExtentSlice ToVolumeExtentSlice() const;

    std::map<uint64_t, PExtent> GetExtentsForTesting() const;

 private:
    uint64_t offset_;
    std::map<uint64_t, PExtent> extents_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_EXTENT_SLICE_H_
