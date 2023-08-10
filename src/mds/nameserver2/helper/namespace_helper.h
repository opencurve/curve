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
 * File Created: Thursday, 28th March 2019 3:16:27 pm
 * Author: tongguangxun
 */

#ifndef SRC_MDS_NAMESERVER2_HELPER_NAMESPACE_HELPER_H_
#define SRC_MDS_NAMESERVER2_HELPER_NAMESPACE_HELPER_H_
#include <string>

#include "src/common/encode.h"
#include "proto/nameserver2.pb.h"
#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {

class NameSpaceStorageCodec {
 public:
    static std::string EncodeFileStoreKey(uint64_t parentID,
                                const std::string &fileName);
    static std::string EncodeSnapShotFileStoreKey(uint64_t parentID,
                                const std::string &fileName);
    static std::string EncodeSegmentStoreKey(uint64_t inodeID, offset_t offset);

    static bool EncodeFileInfo(const FileInfo &finlInfo, std::string *out);
    static bool DecodeFileInfo(const std::string info, FileInfo *fileInfo);
    static bool EncodeSegment(const PageFileSegment &segment, std::string *out);
    static bool DecodeSegment(const std::string info, PageFileSegment *segment);
    static std::string EncodeID(uint64_t value);
    static bool DecodeID(const std::string &value, uint64_t *out);

    static std::string EncodeSegmentAllocKey(uint16_t lid);
    static std::string EncodeSegmentAllocValue(uint16_t lid, uint64_t alloc);
    static bool DecodeSegmentAllocValue(
        const std::string &value, uint16_t *lid, uint64_t *alloc);
};

inline bool isPathValid(const std::string path) {
    if (path.empty() || path[0] != '/') {
        return false;
    }

    if (path.size() > 1U && path[path.size() - 1] == '/') {
        return false;
    }

    bool slash = false;
    for (uint32_t i = 0; i < path.size(); i++) {
        if (path[i] == '/') {
            if (slash) {
                return false;
            }
            slash = true;
        } else {
            slash = false;
        }
    }

    // if some other limits to path can add here in the future
    return true;
}

const std::string kSnapshotPathSeprator = "/";
const std::string kSnapshotSeqSeprator = "-";

inline std::string MakeSnapshotName(const std::string &fileName, FileSeqType seq) {
    return fileName + kSnapshotSeqSeprator + std::to_string(seq);
}

bool SplitSnapshotPath(const std::string &snapFilePath,
    std::string *filePath, FileSeqType *seq);

}   // namespace mds
}   // namespace curve

#endif  // SRC_MDS_NAMESERVER2_HELPER_NAMESPACE_HELPER_H_
