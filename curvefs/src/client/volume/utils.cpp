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
 * Date: Friday Mar 18 11:31:14 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/client/volume/utils.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <vector>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/volume/common.h"

namespace curvefs {
namespace client {

using ::curvefs::metaserver::Inode;
using ::curvefs::volume::AllocateHint;

bool AllocSpace(SpaceManager* space,
                const AllocPart& part,
                std::map<uint64_t, WritePart>* writes,
                std::map<uint64_t, Extent>* alloc) {
VLOG(0) << "whs AllocSpace start";
    AllocateHint hint;
    if (part.allocInfo.leftHintAvailable) {
        hint.leftOffset = part.allocInfo.pOffsetLeft;
    }

    if (part.allocInfo.rightHintAvailable) {
        hint.rightOffset = part.allocInfo.pOffsetRight;
    }

    std::vector<Extent> exts;
    auto ret = space->Alloc(part.allocInfo.len, hint, &exts);
    if (!ret) {
        LOG(ERROR) << "allocate space error, length: " << part.allocInfo.len
                   << ", hint: " << hint;
        return false;
    }

    // may allocate more than one extents for one alloc request
    if (exts.size() == 1) {
        WritePart newPart;
        newPart.offset = exts[0].offset + part.padding;
        newPart.length = part.writelength;
        newPart.data = part.data;

        writes->emplace(part.allocInfo.lOffset, newPart);
        alloc->emplace(part.allocInfo.lOffset, exts[0]);
VLOG(0) << "whs AllocSpace end00";
        return true;
    }

    const char* datap = part.data;

    uint64_t loffset = part.allocInfo.lOffset;
    uint64_t totalsize = 0;

    int64_t writelength = part.writelength;
    size_t padding = part.padding;

    for (const auto& ext : exts) {
        if (writelength > 0) {
            WritePart newPart;
            newPart.offset = ext.offset + padding;
            if (static_cast<uint64_t>(writelength) < ext.len) {
                newPart.length = writelength;
            } else {
                newPart.length = ext.len - padding;
            }
            newPart.data = datap;

            writes->emplace(loffset, newPart);
            datap += newPart.length;
            writelength -= newPart.length;
            padding = 0;
        }

        alloc->emplace(loffset, ext);

        loffset += ext.len;
        totalsize += ext.len;
    }

    CHECK(part.allocInfo.len == totalsize);
VLOG(0) << "whs AllocSpace end01";
    return true;
}

bool PrepareWriteRequest(off_t off,
                         size_t size,
                         const char* data,
                         ExtentCache* extentCache,
                         SpaceManager* spaceManager,
                         std::vector<WritePart>* writes) {
    std::vector<AllocPart> needalloc;
 LOG(ERROR) << "whs PrepareWriteRequest: " << off
            << ", " << size;
    extentCache->DivideForWrite(off, size, data, writes, &needalloc);

    if (needalloc.empty()) {
        LOG(ERROR) << "whs PrepareWriteRequest 00: " << off
            << ", " << size;
        return true;
    }
LOG(ERROR) << "whs PrepareWriteRequest 01: " << off
            << ", " << size;
    std::map<uint64_t, WritePart> newalloc;
    std::map<uint64_t, Extent> newextents;

    // alloc enough space for write
    for (const auto& alloc : needalloc) {
        VLOG(0) << "whs PrepareWriteRequest need alloc ";
      
        auto ret = AllocSpace(spaceManager, alloc, &newalloc, &newextents);
        if (!ret) {
            LOG(ERROR) << "Alloc space error";
            return false;
        }
    }

LOG(ERROR) << "whs PrepareWriteRequest 03: " << off
            << ", " << size;

    // insert allocated space into extent cache
    for (const auto& ext : newextents) {
LOG(ERROR) << "whs PrepareWriteRequest 04 need merge";
        PExtent pext{ext.second.len, ext.second.offset, true};
        extentCache->Merge(ext.first, pext);
    }

    for (const auto& alloc : newalloc) {
LOG(ERROR) << "whs PrepareWriteRequest 05 need do";
        writes->emplace_back(alloc.second.offset, alloc.second.length,
                             alloc.second.data);
    }

 LOG(ERROR) << "whs PrepareWriteRequest end: " << off
            << ", " << size;

    return true;
}

}  // namespace client
}  // namespace curvefs
