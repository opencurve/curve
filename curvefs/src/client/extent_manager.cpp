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

#include "glog/logging.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR SimpleExtentManager::GetToAllocExtents(
    const VolumeExtentList &extents, uint64_t offset, uint64_t len,
    std::list<ExtentAllocInfo> *toAllocExtents) {
    toAllocExtents->clear();
    uint64_t pOffsetLeft = 0;
    bool leftHintAvailable = false;
    for (int i = 0; i < extents.volumeextents_size(); i++) {
        uint64_t left = extents.volumeextents(i).fsoffset();
        uint64_t right = left + extents.volumeextents(i).length();
        if (offset >= left) {
            //
            // extent of [offset, len]                      |------|
            // volumeextents               ...    |-------|     ...
            //
            if (offset > right) {
                pOffsetLeft = 0;
                leftHintAvailable = false;
                continue;
                //
                // extent of [offset, len]                  |------|
                // volumeextents               ...    |-------|     ...
                //
            } else if ((offset + len) > right) {
                len = offset + len - right;
                offset = right;
                pOffsetLeft = extents.volumeextents(i).volumeoffset() +
                              extents.volumeextents(i).length();
                leftHintAvailable = true;
                continue;
                //
                // extent of [offset, len]              |------|
                // volumeextents               ...    |----------|     ...
                //
            } else {
                return CURVEFS_ERROR::OK;
            }
        } else if ((offset + len) >= left) {
            ExtentAllocInfo allocInfo;
            allocInfo.lOffset = offset;
            allocInfo.len = left - offset;
            allocInfo.leftHintAvailable = leftHintAvailable;
            allocInfo.pOffsetLeft = pOffsetLeft;
            allocInfo.rightHintAvailable = true;
            allocInfo.pOffsetRight = extents.volumeextents(i).volumeoffset();
            toAllocExtents->push_back(allocInfo);
            //
            // extent of [offset, len]         |-------------|
            // volumeextents               ...    |-------|     ...
            //
            if ((offset + len) > right) {
                len = offset + len - right;
                offset = right;
                pOffsetLeft = extents.volumeextents(i).volumeoffset() +
                              extents.volumeextents(i).length();
                leftHintAvailable = true;
                continue;
                //
                // extent of [offset, len]         |-------|
                // volumeextents               ...    |-------|     ...
                //
            } else {
                return CURVEFS_ERROR::OK;
            }
            //
            // extent of [offset, len]      |-------|
            // volumeextents                   ...    |-------|     ...
            //
        } else {
            ExtentAllocInfo allocInfo;
            allocInfo.lOffset = offset;
            allocInfo.len = len;
            allocInfo.leftHintAvailable = leftHintAvailable;
            allocInfo.pOffsetLeft = pOffsetLeft;
            allocInfo.rightHintAvailable = false;
            allocInfo.pOffsetRight = 0;
            toAllocExtents->push_back(allocInfo);
            return CURVEFS_ERROR::OK;
        }
    }
    // the extent of [offset, len] is at the right of all volumeextents.
    if (len > 0) {
        // prealloc
        if (len > preAllocSize_) {
            len = (len / preAllocSize_ + 1) * preAllocSize_;
        } else {
            len = preAllocSize_;
        }
        ExtentAllocInfo allocInfo;
        allocInfo.lOffset = offset;
        allocInfo.len = len;
        allocInfo.leftHintAvailable = leftHintAvailable;
        allocInfo.pOffsetLeft = pOffsetLeft;
        allocInfo.rightHintAvailable = false;
        allocInfo.pOffsetRight = 0;
        toAllocExtents->push_back(allocInfo);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR SimpleExtentManager::MergeAllocedExtents(
    const std::list<ExtentAllocInfo> &toAllocExtents,
    const std::list<Extent> &allocatedExtents, VolumeExtentList *extents) {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    VolumeExtentList newExtents;
    auto it = toAllocExtents.begin();
    auto ix = allocatedExtents.begin();
    int i = 0;
    while (i < extents->volumeextents_size() || it != toAllocExtents.end()) {
        if ((it == toAllocExtents.end()) ||
            ((i < extents->volumeextents_size()) &&
             (extents->volumeextents(i).fsoffset() <= it->lOffset))) {
            ret = MergeToTheLastOrAdd(&newExtents, &extents->volumeextents(i));
            if (ret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                return ret;
            }
            i++;
        } else {
            uint64_t len = 0;
            uint64_t lOffset = it->lOffset;
            while (len < it->len && ix != allocatedExtents.end()) {
                VolumeExtent temp;
                temp.set_fsoffset(lOffset);
                temp.set_volumeoffset(ix->offset());
                temp.set_length(ix->length());
                temp.set_isused(false);
                ret = MergeToTheLastOrAdd(&newExtents, &temp);
                if (ret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                    return ret;
                }
                len += ix->length();
                lOffset += ix->length();
                ix++;
            }
            if (len != it->len) {
                LOG(ERROR) << "MergeAllocedExtents find toAllocExtents and"
                           << " allocatedExtents not match, "
                           << "need len = " << it->len
                           << ", actual len = " << len;
                return CURVEFS_ERROR::INTERNAL;
            }
            it++;
        }
    }
    extents->Swap(&newExtents);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR
SimpleExtentManager::MergeToTheLastOrAdd(VolumeExtentList *extents,
                                         const VolumeExtent *extent) {
    VLOG(6) << "MergeToTheLastOrAdd newExtent {"
            << "lOffset = " << extent->fsoffset()
            << ", pOffset = " << extent->volumeoffset()
            << ", length = " << extent->length()
            << ", isused = " << extent->isused() << "}";

    if (extents->volumeextents_size() == 0) {
        VolumeExtent *ext = extents->add_volumeextents();
        ext->CopyFrom(*extent);
        return CURVEFS_ERROR::OK;
    }
    int last = extents->volumeextents_size() - 1;
    uint64_t lLeft = extents->volumeextents(last).fsoffset();
    uint64_t lRight = lLeft + extents->volumeextents(last).length();
    uint64_t pLeft = extents->volumeextents(last).volumeoffset();
    uint64_t pRight = pLeft + extents->volumeextents(last).length();
    if (extent->fsoffset() < lRight) {
        LOG(ERROR) << "The new extent is overlaped, "
                   << " fsoffset = " << extent->fsoffset()
                   << " is < the last extent lRight = " << lRight;
        return CURVEFS_ERROR::INTERNAL;
    }
    if (extent->fsoffset() == lRight && extent->volumeoffset() == pRight) {
        if (extent->isused() == extents->volumeextents(last).isused()) {
            uint64_t newLen = extent->fsoffset() + extent->length() -
                              extents->volumeextents(last).fsoffset();
            extents->mutable_volumeextents(last)->set_length(newLen);
            return CURVEFS_ERROR::OK;
        }
    }
    VolumeExtent *ext = extents->add_volumeextents();
    ext->CopyFrom(*extent);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR
SimpleExtentManager::MarkExtentsWritten(uint64_t offset, uint64_t len,
                                        VolumeExtentList *extents) {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    VolumeExtentList newExtents;
    int i = 0;
    for (; i < extents->volumeextents_size() && len != 0; i++) {
        uint64_t lLeft = extents->volumeextents(i).fsoffset();
        uint64_t length = extents->volumeextents(i).length();
        uint64_t lRight = lLeft + length;
        uint64_t pLeft = extents->volumeextents(i).volumeoffset();
        bool isused = extents->volumeextents(i).isused();
        //
        // extent of [offset, len]      |------ ...
        // volumeextents                   ...    |-------|     ...
        //
        if (offset < lLeft) {
            LOG(ERROR) << "MarkExtentsWritten find write [offset len]"
                       << "is not allocated, offset = " << offset
                       << ", len = " << len
                       << ", right neighbor lLeft = " << lLeft
                       << ", lRight = " << lRight;
            return CURVEFS_ERROR::INTERNAL;
            //
            // extent of [offset, len]                          |-------|
            // volumeextents                   ...    |-------|     ...
            //
        } else if (offset >= lRight) {
            ret = MergeToTheLastOrAdd(&newExtents, &extents->volumeextents(i));
            if (ret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                return ret;
            }
            //
            // extent of [offset, len]                  |--- ...
            // volumeextents                   ...    |-------|     ...
            //
        } else {
            VolumeExtent currentExtent;
            currentExtent.CopyFrom(extents->volumeextents(i));
            if (offset > lLeft) {
                if (!isused) {
                    // cut currentExtent's left part
                    VolumeExtent left;
                    left.set_fsoffset(lLeft);
                    left.set_volumeoffset(pLeft);
                    left.set_length(offset - lLeft);
                    left.set_isused(false);
                    ret = MergeToTheLastOrAdd(&newExtents, &left);
                    if (ret != CURVEFS_ERROR::OK) {
                        LOG(ERROR)
                            << "MergeToTheLastOrAdd failed, ret = " << ret;
                        return ret;
                    }

                    pLeft = pLeft + (offset - lLeft);
                    currentExtent.set_volumeoffset(pLeft);
                    lLeft = offset;
                    currentExtent.set_fsoffset(lLeft);
                    length = lRight - lLeft;
                    currentExtent.set_length(length);
                    currentExtent.set_isused(isused);
                }
            }
            //
            // extent of [offset, len]                     |-------|
            // volumeextents                   ...    |-------|     ...
            //
            if (offset + len >= lRight) {
                currentExtent.set_isused(true);
                ret = MergeToTheLastOrAdd(&newExtents, &currentExtent);
                if (ret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                    return ret;
                }
                // cut [offset len] left part
                len = offset + len - lRight;
                offset = lRight;
                //
                // extent of [offset, len]                  |-------|
                // volumeextents                   ...    |-----------|     ...
                //
            } else {
                // cut currentExtent's right part
                currentExtent.set_length(len);
                currentExtent.set_isused(true);

                VolumeExtent right;
                right.set_fsoffset(lLeft + len);
                right.set_volumeoffset(pLeft + len);
                right.set_length(length - len);
                right.set_isused(isused);
                ret = MergeToTheLastOrAdd(&newExtents, &currentExtent);
                if (ret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                    return ret;
                }
                ret = MergeToTheLastOrAdd(&newExtents, &right);
                if (ret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                    return ret;
                }
                len = 0;
            }
        }
    }
    for (; i < extents->volumeextents_size(); i++) {
        VolumeExtent *ext = newExtents.add_volumeextents();
        ext->CopyFrom(extents->volumeextents(i));
    }
    extents->Swap(&newExtents);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR
SimpleExtentManager::DivideExtents(const VolumeExtentList &extents,
                                   uint64_t offset, uint64_t len,
                                   std::list<PExtent> *pExtents) {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    pExtents->clear();
    for (int i = 0; i < extents.volumeextents_size() && len != 0; i++) {
        uint64_t lLeft = extents.volumeextents(i).fsoffset();
        uint64_t length = extents.volumeextents(i).length();
        uint64_t lRight = lLeft + length;
        uint64_t pLeft = extents.volumeextents(i).volumeoffset();
        uint64_t pRight = pLeft + length;
        bool isused = extents.volumeextents(i).isused();
        //
        // extent of [offset, len]      |------ ...
        // volumeextents                   ...    |-------|     ...
        //
        if (offset < lLeft) {
            // TODO(xuchaojie) : fix when have hole
            LOG(WARNING) << "DivideExtents find [offset len]"
                         << "is not allocated, offset = " << offset
                         << ", len = " << len
                         << ", right neighbor lLeft = " << lLeft
                         << ", lRight = " << lRight;
            return CURVEFS_ERROR::INTERNAL;
            //
            // extent of [offset, len]                          |-------|
            // volumeextents                   ...    |-------|     ...
            //
        } else if (offset >= lRight) {
            continue;
        } else {
            //
            // extent of [offset, len]                     |-------|
            // volumeextents                   ...    |-------|     ...
            //
            if (offset + len >= lRight) {
                PExtent pExt;
                pExt.pOffset = pLeft + (offset - lLeft);
                pExt.len = pRight - pExt.pOffset;
                pExt.UnWritten = !isused;
                ret = MergeToTheLastOrAdd(pExtents, pExt);
                if (ret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                    return ret;
                }
                len = offset + len - lRight;
                offset = lRight;
                //
                // extent of [offset, len]                  |-------|
                // volumeextents                   ...    |-----------|     ...
                //
            } else {
                PExtent pExt;
                pExt.pOffset = pLeft + (offset - lLeft);
                pExt.len = len;
                pExt.UnWritten = !isused;
                ret = MergeToTheLastOrAdd(pExtents, pExt);
                if (ret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "MergeToTheLastOrAdd failed, ret = " << ret;
                    return ret;
                }
                break;
            }
        }
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR
SimpleExtentManager::MergeToTheLastOrAdd(std::list<PExtent> *pExtents,
                                         const PExtent &pExt) {
    VLOG(6) << "MergeToTheLastOrAdd pExtent {"
            << "pOffset = " << pExt.pOffset << ", len = " << pExt.len
            << ", UnWritten = " << pExt.UnWritten;
    if (pExtents->empty()) {
        pExtents->push_back(pExt);
        return CURVEFS_ERROR::OK;
    }
    auto last = pExtents->back();
    if (pExt.pOffset == last.pOffset + last.len) {
        if (pExt.UnWritten == last.UnWritten) {
            last.len += pExt.len;
            return CURVEFS_ERROR::OK;
        }
    }
    pExtents->push_back(pExt);
    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs
