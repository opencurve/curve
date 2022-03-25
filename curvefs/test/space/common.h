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

#ifndef CURVEFS_TEST_SPACE_COMMON_H_
#define CURVEFS_TEST_SPACE_COMMON_H_

#include <glog/logging.h>

#include <algorithm>
#include <numeric>
#include <vector>

#include "curvefs/src/space/free_extents.h"

namespace curvefs {
namespace space {

inline uint64_t TotalLength(const Extents& exts) {
    return std::accumulate(
        exts.begin(), exts.end(), 0ull,
        [](uint64_t v, const PExtent& e) { return v + e.len; });
}

inline bool ExtentsNotOverlap(Extents exts) {
    std::sort(exts.begin(), exts.end(),
              [](const PExtent& e1, const PExtent& e2) {
                  return e1.offset < e2.offset;
              });

    for (size_t i = 1; i < exts.size(); ++i) {
        if (exts[i - 1].offset + exts[i - 1].len > exts[i].offset) {
            return false;
        }
    }

    return true;
}

inline bool ExtentsContinuous(Extents exts) {
    std::sort(exts.begin(), exts.end(),
              [](const PExtent& e1, const PExtent& e2) {
                  return e1.offset < e2.offset;
              });

    for (size_t i = 1; i < exts.size(); ++i) {
        if (exts[i - 1].offset + exts[i - 1].len != exts[i].offset) {
            return false;
        }
    }

    return true;
}

inline Extents SortAndMerge(Extents exts) {
    if (exts.empty() || exts.size() == 1) {
        return exts;
    }

    Extents tmp;

    std::sort(exts.begin(), exts.end(),
              [](const PExtent& e1, const PExtent& e2) {
                  return e1.offset < e2.offset;
              });

    PExtent current = exts[0];
    for (size_t i = 1; i < exts.size(); ++i) {
        if (current.offset + current.len == exts[i].offset) {
            current.len += exts[i].len;
        } else {
            tmp.push_back(current);
            current = exts[i];
        }
    }

    tmp.push_back(current);

    return tmp;
}

inline bool IsEqual(const ::google::protobuf::RepeatedPtrField<
                        ::curvefs::space::Extent>& protoExts,
                    const Extents& phyExtents) {
    if (static_cast<size_t>(protoExts.size()) != phyExtents.size()) {
        return false;
    }

    for (int i = 0; i < protoExts.size(); ++i) {
        if (protoExts[i].offset() != phyExtents[i].offset ||
            protoExts[i].length() != phyExtents[i].len) {
            return false;
        }
    }

    return true;
}

inline bool operator==(const SpaceAllocateHint& h1,
                       const SpaceAllocateHint& h2) {
    return h1.allocType == h2.allocType && h1.leftOffset == h2.leftOffset &&
           h1.rightOffset && h2.rightOffset;
}

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_TEST_SPACE_COMMON_H_
