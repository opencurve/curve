/*
 * Project: curve
 * File Created: Thursday, 28th March 2019 3:16:36 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <vector>
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/string_util.h"

namespace curve {
namespace mds {

const char FILEINFOKEYPREFIX[] = "01";
const char FILEINFOKEYEND[] = "02";
const char SEGMENTINFOKEYPREFIX[] = "02";
const char SEGMENTINFOKEYEND[] = "03";
const char SNAPSHOTFILEINFOKEYPREFIX[] = "03";
const char SNAPSHOTFILEINFOKEYEND[] = "04";
const char INODESTOREKEY[] = "04";
const char INODESTOREKEYEND[] = "05";
const char CHUNKSTOREKEY[] = "05";
const char CHUNKSTOREKEYEND[] = "06";
const char MDSLEADERCAMPAIGNNPFX[] = "07leader";
const char SEGMENTALLOCSIZEKEY[] = "08";
const char SEGMENTALLOCSIZEKEYEND[] = "09";

// TODO(hzsunjianliang): if use single prefix for snapshot file?
const int PREFIX_LENGTH = 2;
const int SEGMENTKEYLEN = 18;

std::string NameSpaceStorageCodec::EncodeFileStoreKey(uint64_t parentID,
                                                const std::string &fileName) {
    std::string storeKey;
    storeKey.resize(PREFIX_LENGTH + sizeof(parentID) + fileName.length());

    memcpy(&(storeKey[0]), FILEINFOKEYPREFIX,  PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), parentID);
    memcpy(&(storeKey[10]), fileName.data(), fileName.length());
    return storeKey;
}

std::string NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(uint64_t parentID,
                                                const std::string &fileName) {
    std::string storeKey;
    storeKey.resize(PREFIX_LENGTH + sizeof(parentID) + fileName.length());

    memcpy(&(storeKey[0]), SNAPSHOTFILEINFOKEYPREFIX, PREFIX_LENGTH);
    ::curve::common::EncodeBigEndian(&(storeKey[2]), parentID);
    memcpy(&(storeKey[10]), fileName.data(), fileName.length());
    return storeKey;
}

std::string NameSpaceStorageCodec::EncodeSegmentStoreKey(uint64_t inodeID,
                                                   offset_t offset) {
    std::string storeKey;
    storeKey.resize(SEGMENTKEYLEN);
    memcpy(&(storeKey[0]), SEGMENTINFOKEYPREFIX,  PREFIX_LENGTH);
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
