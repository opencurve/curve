/*
 * Project: curve
 * File Created: Thursday, 28th March 2019 3:16:27 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_HELPER_NAMESPACE_HELPER_H_
#define SRC_MDS_NAMESERVER2_HELPER_NAMESPACE_HELPER_H_
#include <string>

#include "src/common/encode.h"
#include "proto/nameserver2.pb.h"
#include "src/mds/common/mds_define.h"

namespace curve {
namespace mds {

extern const char FILEINFOKEYPREFIX[];
extern const char FILEINFOKEYEND[];
extern const char SEGMENTINFOKEYPREFIX[];
extern const char SEGMENTINFOKEYEND[];
extern const char SNAPSHOTFILEINFOKEYPREFIX[];
extern const char SNAPSHOTFILEINFOKEYEND[];
extern const char INODESTOREKEY[];
extern const char INODESTOREKEYEND[];
extern const char CHUNKSTOREKEY[];
extern const char CHUNKSTOREKEYEND[];
extern const char MDSLEADERCAMPAIGNNPFX[];
extern const char SEGMENTALLOCSIZEKEY[];
extern const char SEGMENTALLOCSIZEKEYEND[];

extern const int PREFIX_LENGTH;
extern const int SEGMENTKEYLEN;

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
}   // namespace mds
}   // namespace curve

#endif  // SRC_MDS_NAMESERVER2_HELPER_NAMESPACE_HELPER_H_
