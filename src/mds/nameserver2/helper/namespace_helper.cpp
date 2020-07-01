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
 * File Created: Thursday, 28th March 2019 3:16:36 pm
 * Author: tongguangxun
 */

#include <vector>
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/string_util.h"
#include "src/common/namespace_define.h"

using ::curve::common::COMMON_PREFIX_LENGTH;
using ::curve::common::FILEINFOKEYPREFIX;
using ::curve::common::SNAPSHOTFILEINFOKEYPREFIX;
using ::curve::common::SEGMENTKEYLEN;
using ::curve::common::SEGMENTINFOKEYPREFIX;
using ::curve::common::SEGMENTALLOCSIZEKEY;

namespace curve {
namespace mds {
std::string NameSpaceStorageCodec::EncodeFileStoreKey(uint64_t parentID,
                                                const std::string &fileName) {
    std::string storeKey;
    storeKey.resize(
        COMMON_PREFIX_LENGTH + sizeof(parentID) + fileName.length());

    memcpy(&(storeKey[0]), FILEINFOKEYPREFIX,  COMMON_PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), parentID);
    memcpy(&(storeKey[10]), fileName.data(), fileName.length());
    return storeKey;
}

std::string NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(uint64_t parentID,
                                                const std::string &fileName) {
    std::string storeKey;
    storeKey.resize(
        COMMON_PREFIX_LENGTH + sizeof(parentID) + fileName.length());

    memcpy(&(storeKey[0]), SNAPSHOTFILEINFOKEYPREFIX, COMMON_PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), parentID);
    memcpy(&(storeKey[10]), fileName.data(), fileName.length());
    return storeKey;
}

std::string NameSpaceStorageCodec::EncodeSegmentStoreKey(uint64_t inodeID,
                                                   offset_t offset) {
    std::string storeKey;
    storeKey.resize(SEGMENTKEYLEN);
    memcpy(&(storeKey[0]), SEGMENTINFOKEYPREFIX,  COMMON_PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), inodeID);
    ::curve::common::EncodeBigEndian(&(storeKey[10]), offset);
    return storeKey;
}

bool NameSpaceStorageCodec::EncodeFileInfo(const FileInfo &fileInfo,
                                     std::string *out) {
    return fileInfo.SerializeToString(out);
}

bool NameSpaceStorageCodec::DecodeFileInfo(const std::string info,
                                     FileInfo *fileInfo) {
    return fileInfo->ParseFromString(info);
}

bool NameSpaceStorageCodec::EncodeSegment(const PageFileSegment &segment,
                                    std::string *out) {
    return segment.SerializeToString(out);
}

bool NameSpaceStorageCodec::DecodeSegment(const std::string info,
                                    PageFileSegment *segment) {
    return segment->ParseFromString(info);
}

std::string NameSpaceStorageCodec::EncodeID(uint64_t value) {
    return std::to_string(value);
}

bool NameSpaceStorageCodec::DecodeID(
    const std::string &value, uint64_t *out) {
    return ::curve::common::StringToUll(value, out);
}

std::string NameSpaceStorageCodec::EncodeSegmentAllocKey(uint16_t lid) {
    return SEGMENTALLOCSIZEKEY + std::to_string(lid);
}

std::string NameSpaceStorageCodec::EncodeSegmentAllocValue(
    uint16_t lid, uint64_t alloc) {
    return std::to_string(lid) + "_" + std::to_string(alloc);
}

bool NameSpaceStorageCodec::DecodeSegmentAllocValue(
        const std::string &value, uint16_t *lid, uint64_t *alloc) {
    std::vector<std::string> res;
    ::curve::common::SplitString(value, "_", &res);
    if (res.size() != 2) {
        LOG(ERROR) << "segment alloc value: "
                   << value << " is in unknownn format";
        return false;
    }

    uint64_t tmplid;
    bool lidOk = ::curve::common::StringToUll(res[0], &tmplid);
    if (false == lidOk) {
        LOG(ERROR) << "get logicalPoolId from " << res[0] << " fail";
        return false;
    }
    *lid = tmplid;

    bool allocOk = ::curve::common::StringToUll(res[1], alloc);
    if (false == allocOk) {
        LOG(ERROR) << "get alloc value from " << res[1] << " fail";
        return false;
    }

    return true;
}
}   // namespace mds
}   // namespace curve
